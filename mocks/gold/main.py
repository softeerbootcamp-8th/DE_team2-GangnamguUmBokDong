from __future__ import annotations

import argparse
import shutil
from pathlib import Path


ROOT = Path("/tmp/e2e_components")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-key", required=True)

    args = parser.parse_args()

    run_dir = ROOT / args.run_key
    gold_path = run_dir / "gold.json"

    if not gold_path.exists():
        raise FileNotFoundError(gold_path)

    rds_dir = ROOT / "mock_rds" / "forecast_points"
    rds_dir.mkdir(parents=True, exist_ok=True)

    destination = rds_dir / f"{args.run_key}.json"

    # 같은 window 재실행 시 덮어쓰기 → UPSERT 모사
    shutil.copyfile(gold_path, destination)

    print(f"[gold] upserted {destination}")


if __name__ == "__main__":
    main()