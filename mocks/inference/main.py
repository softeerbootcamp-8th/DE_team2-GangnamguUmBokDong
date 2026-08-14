from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path("/tmp/e2e_components")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--run-key", required=True)

    args = parser.parse_args()

    run_dir = ROOT / args.run_key
    input_path = run_dir / "inference_input.json"

    if not input_path.exists():
        raise FileNotFoundError(input_path)

    inference_input = json.loads(input_path.read_text())

    output = {
        "window_start": args.window_start,
        "station_id": inference_input["station_id"],
        "predicted_dttm": args.window_start,
        "predicted_rent_cnt": 5.2,
        "predicted_return_cnt": 3.1,
    }

    output_path = run_dir / "prediction.json"
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2)
    )

    print(f"[inference] wrote {output_path}")


if __name__ == "__main__":
    main()