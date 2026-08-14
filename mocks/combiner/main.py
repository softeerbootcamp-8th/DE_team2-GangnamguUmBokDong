from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path("/tmp/e2e_components")


def build_inference_input(run_dir: Path) -> None:
    silver_path = run_dir / "silver.json"

    if not silver_path.exists():
        raise FileNotFoundError(silver_path)

    silver = json.loads(silver_path.read_text())

    output = {
        "window_start": silver["window_start"],
        "station_id": silver["station_id"],
        "current_bikes": silver["current_bikes"],
        "temperature": silver["temperature"],
        "population": silver["population"],
    }

    output_path = run_dir / "inference_input.json"
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2)
    )

    print(f"[combiner] wrote {output_path}")


def build_serving_output(run_dir: Path) -> None:
    silver_path = run_dir / "silver.json"
    prediction_path = run_dir / "prediction.json"

    if not silver_path.exists():
        raise FileNotFoundError(silver_path)

    if not prediction_path.exists():
        raise FileNotFoundError(prediction_path)

    silver = json.loads(silver_path.read_text())
    prediction = json.loads(prediction_path.read_text())

    output = {
        "sta_id": silver["station_id"],
        "predicted_dttm": prediction["predicted_dttm"],
        "predicted_rent_cnt": prediction["predicted_rent_cnt"],
        "predicted_return_cnt": prediction["predicted_return_cnt"],
        "batch_run_at": prediction["window_start"],
    }

    output_path = run_dir / "gold.json"
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2)
    )

    print(f"[combiner] wrote {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job",
        required=True,
        choices=["inference-input", "serving-output"],
    )
    parser.add_argument("--run-key", required=True)

    args = parser.parse_args()

    run_dir = ROOT / args.run_key

    if args.job == "inference-input":
        build_inference_input(run_dir)
    else:
        build_serving_output(run_dir)


if __name__ == "__main__":
    main()