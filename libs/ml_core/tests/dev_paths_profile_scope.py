"""모델 tick별 station fallback profile의 S3 경로 격리를 검증한다."""

import json
import os
import subprocess
import sys
from pathlib import Path

from ml_core import paths

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _paths_for_ticks(grid_minutes: int, anchor_minutes: int | None = None) -> dict[str, str]:
    """fresh process에서 지정한 model grid/anchor의 주요 산출물 경로를 반환한다."""
    python_paths = [
        str(_REPO_ROOT / "ml"),
        str(_REPO_ROOT / "libs"),
        str(_REPO_ROOT / "libs" / "core" / "src"),
    ]
    if existing := os.environ.get("PYTHONPATH"):
        python_paths.append(existing)
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(python_paths),
        "GRID_TICK_MINUTES": str(grid_minutes),
        "ROLLING_TICK_MINUTES": str(grid_minutes),
    }
    for inherited_name in (
        "FEATURE_PARAM_COMBO_ID",
        "TRAIN_ANCHOR_TICK_MINUTES",
        "MULTI_HORIZON_ANCHOR_TICK_MINUTES",
        "MULTI_HORIZON_ANCHOR_HOURLY_ONLY",
    ):
        env.pop(inherited_name, None)
    if anchor_minutes is not None:
        env["TRAIN_ANCHOR_TICK_MINUTES"] = str(anchor_minutes)
    env.pop("ML_PROFILE", None)
    code = (
        "import json; from ml_core import paths; "
        "print(json.dumps({'station': paths.STATION_HOURLY_PROFILE_PARQUET, "
        "'rental': paths.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(result.stdout)


def test_station_profile_uses_feature_parameter_directory():
    """기본 station profile은 자신을 만든 feature mart와 같은 조합 경로에 있어야 한다."""
    assert paths.STATION_HOURLY_PROFILE_PARQUET == (
        f"{paths.FEATURE_ENGINEERING_OUTPUT_DIR}/station_hourly_profile.parquet"
    )


def test_five_and_twenty_minute_profiles_do_not_overwrite_each_other():
    """5분 A/B profile과 기본 20분 profile은 서로 다른 S3 키를 사용해야 한다."""
    five_minute_path = _paths_for_ticks(5)["station"]
    twenty_minute_path = _paths_for_ticks(20)["station"]

    assert five_minute_path != twenty_minute_path
    assert "/w60_e40_t5/" in five_minute_path
    assert "/w60_e40_t20/" in twenty_minute_path


def test_training_anchor_only_namespaces_multi_horizon_tables():
    """같은 g5의 a5/a20은 base profile을 재사용하고 최종 학습 테이블만 격리한다."""
    anchor_five = _paths_for_ticks(5, 5)
    anchor_twenty = _paths_for_ticks(5, 20)

    assert anchor_five["station"] == anchor_twenty["station"]
    assert anchor_five["rental"] != anchor_twenty["rental"]
    assert "/w60_e40_t5/training_anchor_a5/" in anchor_five["rental"]
    assert "/w60_e40_t5/training_anchor_a20/" in anchor_twenty["rental"]
