"""Collector command-line entry point."""

import argparse
import sys

from sources.bike import fetch_bike_page


SUPPORTED_SOURCES = ("bike",)


def parse_args() -> argparse.Namespace:
    """Parse collector command-line arguments.

    Returns:
        argparse.Namespace: Parsed collector execution arguments.
    """
    parser = argparse.ArgumentParser(description="Run a data collector.")

    parser.add_argument(
        "--source",
        required=True,
        choices=SUPPORTED_SOURCES,
        help="Data source to collect.",
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Logical collection run identifier.",
    )

    return parser.parse_args()


def main() -> int:
    """Run the collector and return an exit code.

    Returns:
        int: Process exit code for Airflow.
    """
    args = parse_args()

    print(f"source={args.source}")
    print(f"run_id={args.run_id}")
    print("collector started")

    payload = fetch_bike_page(1)

    service = payload["rentBikeStatus"]

    print(f"total_count={service['list_total_count']}")
    print(f"row_count={len(service.get('row', []))}")

    print("collector finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())