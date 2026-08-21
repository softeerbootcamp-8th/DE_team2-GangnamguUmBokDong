"""CLI 진입점: 백필(backfill-archive) 및 일자별 생활인구 나우캐스팅(estimate) 실행."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


import pandas as pd
# pyrefly: ignore [missing-import]
import pyarrow as pa

_KST = ZoneInfo("Asia/Seoul")

import backfill
import estimate_day
import holiday
import storage


def _read_archive_as_frame(target_date: date) -> pd.DataFrame | None:
    """지정한 일자의 아카이브 테이블을 읽어 중복을 제거한 DataFrame으로 반환한다."""
    table = storage.read_archive(target_date)
    if table is not None:
        return table.to_pandas().drop_duplicates(subset=["H_DNG_CD", "CELL_ID", "TT"])
    return None


def run_estimate(today: date) -> int:
    """수집된 실측 데이터를 아카이브에 반영하고, D-3부터 D+3까지의 생활인구를 추정한다.
    
    collector의 dt= 파티션은 수집 실행일이지 데이터가 가리키는 실제 날짜가 아니다
    (예: dt=2026-08-15 안의 YMD가 20260811). 
    오늘자 파티션을 읽고 그 안의 YMD로 실제 대상 날짜(biz)를 알아내 그 날짜로 archive에 적재한다.
    """
    # 1. 오늘 수집된 실측 데이터(Silver)를 아카이브로 승격 및 기존 임시 추정치 정리
    real = storage.read_real_grid_silver(today)
    if real is not None:
        # 수집 파일 내 실제 발생 일자(biz_date)별로 분리
        for biz_date, day_table in backfill.group_rows_by_date(real).items():
            # 실측 메타데이터(is_estimated=False, estimation_method='actual') 부여 및 중복 제거
            df = backfill.add_estimation_columns(day_table).to_pandas().drop_duplicates(
                subset=["H_DNG_CD", "CELL_ID", "TT"]
            )
            storage.write_archive(biz_date, pa.Table.from_pandas(df, preserve_index=False))
            # 실측값이 들어왔으므로 기존에 생성해 두었던 임시 나우캐스팅 데이터는 삭제
            storage.delete_nowcast(biz_date)

    # 2. D-3 ~ D+3 일주일 구간에 대해 나우캐스팅(추정치 생성) 수행
    for offset in range(-3, 4):
        target = today + timedelta(days=offset)
        # 이미 실측 아카이브가 존재하는 날짜는 추정할 필요가 없으므로 건너뜀
        if storage.read_archive(target) is not None:
            continue

        # 2-1. 1~4주 전 동일 요일/휴일 패턴의 과거 아카이브 후보 데이터 로드 (주차별 가중평균용)
        candidate_frames = [
            _read_archive_as_frame(candidate) if holiday.matches_target_pattern(candidate, target) else None
            for candidate in holiday.candidate_dates(target)
        ]
        # 후보 리스트 크기를 항상 4개로 고정하여 주차별 가중치 슬롯 유지
        while len(candidate_frames) < 4:
            candidate_frames.append(None)

        # 2-2. 1차 폴백용 5~8주 전 확장 후보 데이터 로드
        extended_frames = [_read_archive_as_frame(d) for d in holiday.extended_candidate_dates(target)]

        # 2-3. 2차 폴백용 과거 전체 동일 패턴 일자들의 격자별 전체 평균 계산
        historical_dates = [d for d in storage.list_archive_dates() if holiday.matches_target_pattern(d, target)]
        historical_avg = estimate_day.historical_average([_read_archive_as_frame(d) for d in historical_dates])
        historical_avg_frame = historical_avg.reset_index() if historical_avg is not None else None

        # 2-4. 가중평균 및 다단계 폴백을 적용하여 최종 나우캐스팅 추정 테이블 생성
        nowcast_df = estimate_day.build_nowcast_table(
            candidate_frames, extended_frames=extended_frames, historical_avg_frame=historical_avg_frame
        )
        if nowcast_df.empty:
            continue
        # 2-5. 추정 결과 테이블을 nowcast 저장소에 저장
        storage.write_nowcast(target, pa.Table.from_pandas(nowcast_df, preserve_index=False))

    return 0


def run_backfill_archive(csv_dir: str) -> int:
    """지정한 디렉터리의 원본 CSV 파일들을 읽어 아카이브 Parquet으로 일괄 적재한다."""
    for csv_path in sorted(Path(csv_dir).glob("*.csv")):
        table = backfill.add_estimation_columns(backfill.read_source_csv(csv_path))
        for target_date, day_table in backfill.group_rows_by_date(table).items():
            storage.write_archive(target_date, day_table)
    return 0


def required_lookback_dates(today: date, horizon_days: int = 3) -> list[date]:
    """나우캐스팅 구간이 참조하는 1~4주 전 후보 날짜를 반환한다.

    ``run_estimate()``는 D-3~D+3을 한 번에 만들기 때문에 기본값도 같은 범위를
    사용한다. 공휴일/일요일은 단순히 7·14·21·28일 전을 고르는 대신 실제 추정과
    동일한 ``holiday.candidate_dates()`` 규칙을 사용해야, 백필은 성공했는데
    estimate가 다른 날짜를 찾는 불일치가 생기지 않는다.
    """
    if horizon_days < 0:
        raise ValueError("horizon_days는 0 이상이어야 한다")

    required: set[date] = set()
    for offset in range(-horizon_days, horizon_days + 1):
        required.update(holiday.candidate_dates(today + timedelta(days=offset)))
    return sorted(required)


def run_bootstrap_lookback(
    csv_dir: str,
    today: date,
    *,
    horizon_days: int = 3,
    force: bool = False,
) -> int:
    """과거 CSV에서 현재 나우캐스팅에 필요한 1~4주 Archive만 초기 적재한다.

    서울시 ``Se250MSpopLocalResd`` API는 과거 날짜 파라미터를 받지 않고 항상 최신
    공표일 한 날짜만 반환한다. 따라서 이 초기 적재는 서울 열린데이터광장에서 받은
    과거 생활인구 CSV를 입력으로 사용한다.

    이미 존재하는 Archive는 기본적으로 유지한다. 필요한 날짜가 CSV와 기존 Archive
    어디에도 없으면 non-zero를 반환해 불완전한 초기화를 성공으로 오인하지 않게 한다.
    """
    csv_paths = sorted(Path(csv_dir).glob("*.csv"))
    if not csv_paths:
        print(f"생활인구 CSV를 찾지 못했습니다: {csv_dir}", file=sys.stderr)
        return 1

    required = set(required_lookback_dates(today, horizon_days))
    available = {day for day in required if storage.read_archive(day) is not None}
    tables_by_date: dict[date, list[pa.Table]] = {}

    for csv_path in csv_paths:
        table = backfill.read_source_csv(csv_path)
        for target_date, day_table in backfill.group_rows_by_date(table).items():
            if target_date in required and (force or target_date not in available):
                tables_by_date.setdefault(target_date, []).append(day_table)

    for target_date, tables in sorted(tables_by_date.items()):
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        combined = backfill.add_estimation_columns(combined)
        frame = combined.to_pandas().drop_duplicates(
            subset=["H_DNG_CD", "CELL_ID", "TT"]
        )
        storage.write_archive(
            target_date, pa.Table.from_pandas(frame, preserve_index=False)
        )
        available.add(target_date)

    missing = sorted(required - available)
    print(
        f"population_lookback target={today} required={len(required)} "
        f"available={len(available)} missing={len(missing)}"
    )
    if missing:
        print(
            "누락 날짜: " + ", ".join(day.isoformat() for day in missing),
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI 인자를 파싱하여 백필 또는 나우캐스팅 추정 명령을 실행한다."""
    # 1. CLI 인자 파서 및 서브 커맨드 정의
    parser = argparse.ArgumentParser(prog="seoul-pop-nowcasting")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1-1. backfill-archive 커맨드: 디렉터리 내 원본 CSV를 아카이브 Parquet으로 일괄 적재
    backfill_parser = subparsers.add_parser("backfill-archive")
    backfill_parser.add_argument("--csv-dir", required=True)

    # 과거 날짜를 지정할 수 없는 생활인구 API 대신 공식 과거 CSV에서, 현재
    # 나우캐스팅에 실제 필요한 1~4주 후보 날짜만 골라 초기 Archive를 만든다.
    lookback_parser = subparsers.add_parser("bootstrap-lookback")
    lookback_parser.add_argument("--csv-dir", required=True)
    lookback_parser.add_argument("--target-date")
    lookback_parser.add_argument(
        "--horizon-days",
        type=int,
        default=3,
        help="target-date 앞뒤 추정 범위(기본 3 = D-3~D+3)",
    )
    lookback_parser.add_argument(
        "--force", action="store_true", help="기존 Archive도 CSV 값으로 다시 쓴다"
    )

    # 1-2. estimate 커맨드: 실측 데이터 승격 및 D-3~D+3 나우캐스팅 추정 실행
    estimate_parser = subparsers.add_parser("estimate")
    estimate_parser.add_argument("--target-date")

    args = parser.parse_args(argv)

    # 2. 커맨드에 따른 분기 실행
    if args.command == "backfill-archive":
        return run_backfill_archive(args.csv_dir)

    if args.command == "bootstrap-lookback":
        today = (
            date.fromisoformat(args.target_date)
            if args.target_date
            else datetime.now(tz=_KST).date()
        )
        return run_bootstrap_lookback(
            args.csv_dir,
            today,
            horizon_days=args.horizon_days,
            force=args.force,
        )

    # estimate 커맨드: target-date가 주어지면 해당 날짜, 없으면 KST 기준 오늘을 기준으로 실행
    today = date.fromisoformat(args.target_date) if args.target_date else datetime.now(tz=_KST).date()
    return run_estimate(today)


if __name__ == "__main__":
    sys.exit(main())
