"""CLI 진입점: backfill-archive(1회성 CSV 적재), estimate(매일 D-3~D+3 추정)."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.csv as pa_csv

_KST = ZoneInfo("Asia/Seoul")

import backfill
import estimate_day
import holiday
import storage


def _read_archive_as_frame(target_date: date):
    table = storage.read_archive(target_date)
    return table.to_pandas() if table is not None else None


def run_estimate(today: date) -> int:
    d4 = today - timedelta(days=4)
    real = storage.read_real_grid_silver(d4)
    if real is not None:
        storage.write_archive(d4, backfill.add_estimation_columns(real))
        storage.delete_nowcast(d4)

    for offset in range(-3, 4):
        target = today + timedelta(days=offset)
        if storage.read_archive(target) is not None:
            continue  # 이미 실측값이 archive에 있음

        candidate_frames = [
            _read_archive_as_frame(candidate) if holiday.matches_target_pattern(candidate, target) else None
            for candidate in holiday.candidate_dates(target)
        ]
        while len(candidate_frames) < 4:
            candidate_frames.append(None)

        extended_frames = [_read_archive_as_frame(d) for d in holiday.extended_candidate_dates(target)]

        historical_dates = [d for d in storage.list_archive_dates() if holiday.matches_target_pattern(d, target)]
        historical_avg = estimate_day.historical_average([_read_archive_as_frame(d) for d in historical_dates])
        historical_avg_frame = historical_avg.reset_index() if historical_avg is not None else None

        nowcast_df = estimate_day.build_nowcast_table(
            candidate_frames, extended_frames=extended_frames, historical_avg_frame=historical_avg_frame
        )
        if nowcast_df.empty:
            continue
        storage.write_nowcast(target, pa.Table.from_pandas(nowcast_df, preserve_index=False))

    return 0


_ID_COLUMNS = ("YMD", "TT", "H_DNG_CD", "CELL_ID")


def run_backfill_archive(csv_dir: str) -> int:
    convert_options = pa_csv.ConvertOptions(column_types={col: pa.string() for col in _ID_COLUMNS})
    for csv_path in sorted(Path(csv_dir).glob("*.csv")):
        table = backfill.add_estimation_columns(pa_csv.read_csv(csv_path, convert_options=convert_options))
        for target_date, day_table in backfill.group_rows_by_date(table).items():
            storage.write_archive(target_date, day_table)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="seoul-pop-nowcasting")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_parser = subparsers.add_parser("backfill-archive")
    backfill_parser.add_argument("--csv-dir", required=True)

    estimate_parser = subparsers.add_parser("estimate")
    estimate_parser.add_argument("--target-date")

    args = parser.parse_args(argv)

    if args.command == "backfill-archive":
        return run_backfill_archive(args.csv_dir)

    today = date.fromisoformat(args.target_date) if args.target_date else datetime.now(tz=_KST).date()
    return run_estimate(today)


if __name__ == "__main__":
    sys.exit(main())
