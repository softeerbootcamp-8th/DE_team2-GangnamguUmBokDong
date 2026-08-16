"""Task 1~6을 실제 데이터(POI shapefile)와 moto S3로 엮은 통합/회귀 테스트.

과거 시도의 실제 파이프라인 출력 fixture는 이 리포지토리에 남아있지 않아
재사용할 수 없었다(이번 조사에서 git 이력·파일시스템 모두 확인). 대신 Task 1에서
커밋한 실제 POI shapefile(POI001="강남 MICE 관광특구")을 그대로 쓰고, 그 폴리곤과
실제로 겹치는/겹치지 않는 CELL_ID를 골라 회귀 시나리오로 고정한다.
"""

import io
from datetime import datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import grid
import main
import merge
import poi
import storage
from tests.conftest import KST, TEST_BUCKET

# POI001("강남 MICE 관광특구")와 실제로 크게 겹치는 격자(약 97.9% 겹침, 이번 조사에서 확인).
OVERLAPPING_CELL_ID = "다사61004575"
# POI001과 전혀 겹치지 않는(충분히 먼) 격자.
PASS_THROUGH_CELL_ID = "다바10759525"

WINDOW_START = datetime(2026, 8, 15, 14, 5, tzinfo=KST)


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


def _seed_grid_silver() -> None:
    rows = {
        "CELL_ID": [OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID],
        "TT": ["14", "14"],
        "H_DNG_CD": ["1168064", "1168064"],
        "SPOP": [200.0, 50.0],
    }
    for c in merge.AGE_COLUMNS:
        rows[c] = [None, None]
    rows["M00"] = [100.0, 25.0]
    rows["F00"] = [100.0, 25.0]

    table = pa.table(rows)
    _put_parquet("silver/living_population_grid/dt=2026-08-15/hh=14/1400.parquet", table)


def _seed_realtime_silver() -> None:
    table = pa.table({
        "AREA_NM": ["강남 MICE 관광특구"],
        "AREA_CD": ["POI001"],
        "AREA_CONGEST_LVL": ["보통"],
        "AREA_PPLTN_MIN": [1000],
        "AREA_PPLTN_MAX": [1400],
        "MALE_PPLTN_RATE": [52.0],
        "FEMALE_PPLTN_RATE": [48.0],
    })
    _put_parquet("silver/population_realtime/dt=2026-08-15/hh=14/1405.parquet", table)


class TestPoi001RealGeometryFixture:
    """이번 조사에서 확인한 실제 겹침/비겹침 관계를, 하드코딩 없이 실제 함수로 재검증한다."""

    def test_overlapping_cell_actually_intersects_poi001(self):
        areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
        poi001 = next(a for a in areas if a.area_cd == "POI001")
        cell_geom = grid.cell_id_to_polygon(OVERLAPPING_CELL_ID)

        intersection_area = cell_geom.intersection(poi001.geometry).area

        assert intersection_area / grid.GRID_AREA_M2 > 0.9  # 90% 이상 겹침

    def test_pass_through_cell_does_not_intersect_poi001(self):
        areas = poi.load_poi_areas(poi.DEFAULT_POI_SHP_PATH)
        poi001 = next(a for a in areas if a.area_cd == "POI001")
        cell_geom = grid.cell_id_to_polygon(PASS_THROUGH_CELL_ID)

        assert not cell_geom.intersects(poi001.geometry)


class TestEndToEndRun:
    def test_run_updates_overlapping_cell_and_passes_through_the_rest(self):
        _seed_grid_silver()
        _seed_realtime_silver()

        exit_code = main.run(WINDOW_START, "strict")

        assert exit_code == 0

        key = "silver/living_population_normalized/dt=2026-08-15/hh=14/1405.parquet"
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        rows = {row["CELL_ID"]: row for row in table.to_pylist()}

        assert set(rows.keys()) == {OVERLAPPING_CELL_ID, PASS_THROUGH_CELL_ID}

        # 겹치지 않는 격자는 원본 그대로 pass-through.
        passthrough = rows[PASS_THROUGH_CELL_ID]
        assert passthrough["SPOP"] == 50
        assert passthrough["M00"] == 25
        assert passthrough["F00"] == 25
        assert passthrough["H_DNG_CD"] == "1168064"

        # 크게 겹치는 격자는 POI 인구추정치((1000+1400)/2=1200) 쪽으로 크게 이동해
        # 원본(200)과는 뚜렷이 달라야 한다.
        overlapping = rows[OVERLAPPING_CELL_ID]
        assert overlapping["SPOP"] != 200
        assert abs(overlapping["SPOP"] - 1200) < abs(200 - 1200)  # POI 쪽으로 이동했는지

        # 출력 스키마: CELL_ID, H_DNG_CD, SPOP + 28개 연령 컬럼, 전부 int64.
        expected_columns = {"CELL_ID", "H_DNG_CD", "SPOP", *merge.AGE_COLUMNS}
        assert set(table.column_names) == expected_columns
        for name in ("SPOP", *merge.AGE_COLUMNS):
            assert table.schema.field(name).type == pa.int64()

    def test_run_writes_manifest_with_baseline_metadata(self):
        _seed_grid_silver()
        _seed_realtime_silver()

        main.run(WINDOW_START, "strict")

        import json
        key = "_manifest/living_population_normalized/dt=2026-08-15/hh=14/1405.json"
        body = json.loads(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())

        assert body["baseline_date"] == "2026-08-15"
        assert body["baseline_date_mode"] == "strict"
        assert body["cell_count"] == 2
        assert body["poi_matched_count"] == 1

    def test_run_raises_when_grid_partition_missing_in_strict_mode(self):
        _seed_realtime_silver()  # grid silver는 심지 않음

        with pytest.raises(storage.PartitionNotFoundError):
            main.run(WINDOW_START, "strict")
