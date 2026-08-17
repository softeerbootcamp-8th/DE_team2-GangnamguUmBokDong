"""로컬 개발/데모용 — feature_engine -> training -> inference 전체를 한 번에 실행한다.

**주의**: 실제 배포에서는 `feature_engine`/`training`/`inference`가 각자 다른
인스턴스에서 따로 실행된다(각 폴더의 README.md 참고) — 세 폴더를 나눈 이유
자체가 그 독립 배포였다. 이 스크립트는 그 원칙을 바꾸는 게 아니라, 로컬
한 대에서 전체 파이프라인이 처음부터 끝까지 정상 동작하는지 빠르게 검증하고
싶을 때만 쓰는 개발 편의용 래퍼다 — 운영 배포 스크립트로 쓰지 말 것.

각 단계는 이미 있는 모듈을 `python -m`으로 그대로 호출한다(로직 중복 없음).
하나라도 실패하면(`check=True`) 그 자리에서 멈춘다. 전체 실행 시간은 원본
데이터 규모 기준 상당히 오래 걸릴 수 있다(피처 생성 수십 분, 모델당 학습
~20분 — history.md 참고) — 급하게 한 단계만 다시 돌리고 싶으면
`--only` 옵션으로 특정 단계만 실행할 수 있다.

**환경은 폴더별 `uv`가 관리한다** — 공용 `.venv`/`.venv-spark`는 안 쓰고, 각
폴더 자기 `.venv`(`<폴더>/.venv/bin/python`, `uv sync`로 미리 준비돼 있어야 함)를
쓴다. `feature_engine`은 Spark로만 한다(`feature_engine/.venv`, 로컬은 `local[*]`
단일 노드 모드) — `feature_engine/.venv`가 없으면 이 단계에서 바로 실패하니 먼저
[feature_engine/README.md](feature_engine/README.md)의 세팅을 따를 것. **1차
정제(pandas)도 포함해서 feature_engine의 pandas 코드 전체가 `feature_engine/legacy/`로
이동했다** — 실제 배포에서는 1차 정제 자체를 이 저장소 밖에서 처리하므로, 아래
첫 스텝(`feature_engine.legacy.scripts.run_build_pipeline`)은 로컬에서
2차정제(Spark)를 테스트해볼 입력을 만드는 용도일 뿐 본 서비스 경로가 아니다 —
배경은 [LEGACY_AUDIT.md](LEGACY_AUDIT.md) 참고.

`libs/ml_core/paths.py`(각 폴더가 editable 의존성으로 참조하는 공유 라이브러리)가
Spark 산출물 경로(`data/processed_v2/spark/{FEATURE_PARAM_COMBO_ID}/...`)를
`feature_engine/spark/config.py`와 정확히 같은 공식으로 계산하므로, dataset 단계가
쓴 파일을 training/inference가 그대로 읽는다(파라미터 조합을 바꾸려면
`FEATURE_ENGINEERING_OUTPUT_ROOT`/`FEATURE_PARAM_COMBO_ID` 환경변수를 두 쪽 다 같이
설정할 것 — LEGACY_AUDIT.md 참고).

실행(이 스크립트 자체는 stdlib만 써서 임의의 python3로 실행 가능 — 각 단계는
내부적으로 해당 폴더의 `.venv`를 골라서 씀):
    python3 run_full_pipeline.py               # 전체
    python3 run_full_pipeline.py --only dataset # feature_engine만
    python3 run_full_pipeline.py --only training
    python3 run_full_pipeline.py --only inference
"""

import argparse
import subprocess
from pathlib import Path

ML_ROOT = Path(__file__).resolve().parent


def _venv_python(folder: str) -> str:
    return str(ML_ROOT / folder / ".venv" / "bin" / "python")


STEPS = {
    "dataset": [
        (
            "feature_engine: 1차 정제(legacy pandas, 1~5단계 — 로컬 테스트 입력 준비용)",
            _venv_python("feature_engine"),
            ["-m", "feature_engine.legacy.scripts.run_build_pipeline"],
        ),
        (
            "feature_engine: 피처마트 생성(Spark, 2차정제 6~8단계, local[*] 단일 노드)",
            _venv_python("feature_engine"),
            ["-m", "feature_engine.spark.run_pipeline"],
        ),
        (
            (
                "feature_engine: multi-horizon 학습 테이블 생성(horizon=1..HORIZON_COUNT self-join, "
                "training이 이제 이 산출물을 읽으므로 빠지면 다음 단계가 실패한다)"
            ),
            _venv_python("feature_engine"),
            ["-m", "feature_engine.spark.build_multi_horizon_features"],
        ),
    ],
    "training": [
        ("training: 대여 모델 학습", _venv_python("training"), ["-m", "training.train_rental_model"]),
        ("training: 반납 모델 학습", _venv_python("training"), ["-m", "training.train_return_model"]),
    ],
    "inference": [
        (
            "inference: 대여/반납 fallback 프로필 생성",
            _venv_python("inference"),
            ["-m", "inference.build_station_profile"],
        ),
        (
            "inference: 인구 fallback 프로필 생성",
            _venv_python("inference"),
            ["-m", "inference.build_population_profile"],
        ),
    ],
}
STAGE_ORDER = ["dataset", "training", "inference"]


def run_stage(stage: str) -> None:
    for label, python_executable, args in STEPS[stage]:
        print(f"\n=== {label} ===", flush=True)
        subprocess.run([python_executable, *args], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="feature_engine -> training -> inference 전체 실행 (로컬 개발용)")
    parser.add_argument(
        "--only", choices=STAGE_ORDER, default=None, help="이 단계만 실행 (미지정 시 전체 순서대로 실행)"
    )
    args = parser.parse_args()

    stages = [args.only] if args.only else STAGE_ORDER
    for stage in stages:
        run_stage(stage)

    print(
        "\n완료 — 예: ./inference/.venv/bin/python -m inference.predict_rental_demand "
        "--station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-07"
    )


if __name__ == "__main__":
    main()
