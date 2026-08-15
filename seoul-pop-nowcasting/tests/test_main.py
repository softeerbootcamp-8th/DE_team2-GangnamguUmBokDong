"""main.py: CLI 서브커맨드 및 estimate/backfill-archive 오케스트레이션."""

import io
from datetime import date, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

import main
import storage
from tests.conftest import TEST_BUCKET


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


def _key_exists(key: str) -> bool:
    try:
        _s3().head_object(Bucket=TEST_BUCKET, Key=key)
        return True
    except ClientError:
        return False


class TestRunEstimateArchivesD4Data:
    def test_archives_real_data_from_todays_partition_using_actual_ymd(self):
        today = date(2026, 8, 20)
        # collector의 dt= 파티션은 "수집 실행일"이다. 실제 대상(biz) 날짜는
        # 그 안의 YMD 컬럼 값으로만 알 수 있다(예: 오늘 실행분의 YMD가 D-4).
        biz_date = today - timedelta(days=4)  # 2026-08-16

        _put_parquet(
            f"silver/living_population_grid/dt={today:%Y-%m-%d}/hh=00/0000.parquet",
            pa.table(
                {
                    "YMD": [f"{biz_date:%Y%m%d}"],
                    "H_DNG_CD": ["H1"],
                    "CELL_ID": ["C1"],
                    "TT": ["10"],
                    "SPOP": [123.0],
                }
            ),
        )
        # 예전에 이 날짜를 추정치로 채워뒀던 잔재
        storage.write_nowcast(biz_date, pa.table({"CELL_ID": ["C1"], "SPOP": [999.0]}))

        main.run_estimate(today)

        archived = storage.read_archive(biz_date)
        assert archived is not None
        assert archived.column("is_estimated").to_pylist() == [False]
        assert storage.nowcast_exists(biz_date) is False


class TestRunEstimateWritesNowcast:
    def test_weighted_average_written_for_missing_date_with_full_history(self):
        today = date(2026, 8, 10)  # 월요일
        target = today + timedelta(days=3)  # 2026-08-13(목, 평일) - D+3

        for weeks_ago, value in [(1, 100.0), (2, 200.0), (3, 300.0), (4, 400.0)]:
            candidate = target - timedelta(weeks=weeks_ago)
            storage.write_archive(
                candidate,
                pa.table({"H_DNG_CD": ["H1"], "CELL_ID": ["C1"], "TT": ["10"], "SPOP": [value]}),
            )

        main.run_estimate(today)

        key = f"silver/living_population_grid/dt={target:%Y-%m-%d}/hh=00/nowcast.parquet"
        assert _key_exists(key)
        stored = pq.read_table(io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()))
        assert stored.column("SPOP").to_pylist() == pytest.approx(
            [100.0 * 0.4 + 200.0 * 0.3 + 300.0 * 0.2 + 400.0 * 0.1]
        )
        assert stored.column("estimation_method").to_pylist() == ["weighted_avg"]

    def test_skips_date_that_already_has_archived_actual(self):
        today = date(2026, 8, 12)
        target = today + timedelta(days=3)
        storage.write_archive(
            target, pa.table({"H_DNG_CD": ["H1"], "CELL_ID": ["C1"], "TT": ["10"], "SPOP": [500.0]})
        )

        main.run_estimate(today)

        key = f"silver/living_population_grid/dt={target:%Y-%m-%d}/hh=00/nowcast.parquet"
        assert _key_exists(key) is False


_PORTAL_HEADER = (
    '"일자","시간","행정동코드","250M격자","생활인구합계","남자 0~9세","남자 10~14세",'
    '"남자 15~19세","남자 20~24세","남자 25~29세","남자 30~34세","남자 35~39세",'
    '"남자 40~44세","남자 45~49세","남자 50~54세","남자 55~59세","남자 60~64세",'
    '"남자 65~69세","남자 70세 이상","여자 0~9세","여자 10~14세","여자 15~19세",'
    '"여자 20~24세","여자 25~29세","여자 30~34세","여자 35~39세","여자 40~44세",'
    '"여자 45~49세","여자 50~54세","여자 55~59세","여자 60~64세","여자 65~69세",'
    '"여자 70세 이상"'
)


def _portal_row(ymd: str, cell_id: str, spop: str) -> str:
    return f'"{ymd}","10","1100053","{cell_id}","{spop}"' + ',"*"' * 28


class TestRunBackfillArchive:
    def test_reads_real_portal_csv_and_writes_one_archive_file_per_date(self, tmp_path):
        csv_path = tmp_path / "250_LOCAL_RESD_sample.csv"
        content = "\n".join(
            [
                _PORTAL_HEADER,
                _portal_row("20260810", "C1", "111.0"),
                _portal_row("20260811", "C1", "222.0"),
            ]
        )
        csv_path.write_bytes((content + "\n").encode("euc-kr"))

        main.run_backfill_archive(str(tmp_path))

        first = storage.read_archive(date(2026, 8, 10))
        second = storage.read_archive(date(2026, 8, 11))
        assert first.column("SPOP").to_pylist() == [111.0]
        assert first.column("is_estimated").to_pylist() == [False]
        assert second.column("SPOP").to_pylist() == [222.0]


class TestCliDispatch:
    def test_estimate_command_calls_run_estimate_with_parsed_date(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(main, "run_estimate", lambda today: captured.setdefault("today", today))

        main.main(["estimate", "--target-date", "2026-08-12"])

        assert captured["today"] == date(2026, 8, 12)

    def test_backfill_archive_command_calls_run_backfill_archive_with_csv_dir(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            main, "run_backfill_archive", lambda csv_dir: captured.setdefault("csv_dir", csv_dir)
        )

        main.main(["backfill-archive", "--csv-dir", "/tmp/some-dir"])

        assert captured["csv_dir"] == "/tmp/some-dir"
