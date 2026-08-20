"""하이퍼파라미터 프로필을 S3에 생성/조회하고 MLflow에 변경 이력을 남긴다.

**`common_config.py`의 실제 런타임 조회는 이 모듈을 쓰지 않는다** — `common_config.
_load_profile()`은 boto3만으로 최소 구현돼 있다(그 파일 docstring 참고: pandas/pyarrow를
끌어오는 `core.s3`를 무거운 의존성 없는 상수 모듈에 넣지 않으려고). 이 모듈은 반대로
operator가 프로필을 "관리"(생성/수정/이력 조회)할 때만 쓰는 진입점이라 `core.s3`/`mlflow`를
자유롭게 쓴다.

**MLflow의 역할**: 실제 서비스(feature_engine/training/inference)가 참조하는 값은 항상
S3(`profiles/{name}.json`)다 — MLflow에 남기는 기록은 "언제 누가 어떤 값으로 바꿨는지"를
보는 감사/이력용이고, 그 자체가 서비스 동작에 반영되지는 않는다. 다만 이 기록도 실제로는
S3에 쌓인다 — MLflow 서버가 아티팩트 저장소를 이미 S3로 프록시하고 있다
(`ops/compose/docker-compose.yml`의 `--serve-artifacts`, `docs/ml/MLFLOW_SETUP.md` 참고).
"""

import mlflow
from core import s3 as s3_io

from . import mlflow_tracking
from .paths import PROFILES_PREFIX, profile_path

PROFILE_MANAGEMENT_EXPERIMENT_NAME = "bike-demand-profiles"


def _flatten(d: dict, prefix: str = "") -> dict:
    """중첩 dict(예: LGB_PARAMS_COMMON)를 `mlflow.log_params()`가 받는 스칼라 dict로 편다."""
    flat = {}
    for key, value in d.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten(value, full_key))
        else:
            flat[full_key] = value
    return flat


def push_profile(name: str, profile: dict) -> None:
    """프로필을 S3에 쓰고(`profiles/{name}.json`), 같은 값을 MLflow에도 기록한다.

    S3 쓰기가 실제 반영이다 — feature_engine/training/inference는 전부 매번 새로 뜨는
    배치 프로세스라, 이 함수 호출 다음부터 시작되는 실행은 자동으로 새 값을 읽는다
    (재배포 불필요, 코드 변경 불필요 — 값만 바뀜).

    args:
        name: 프로필 이름(`ML_PROFILE` 환경변수와 매칭)
        profile: `libs/ml_core/common_config.py`의 `_DEFAULT_PROFILE`과 같은 키 집합을
            갖는 dict(임베고/tick/LGB 파라미터/TRAIN_LOOKBACK_MONTHS 등)
    """
    s3_io.write_json(profile_path(name), profile)
    mlflow_tracking.configure(PROFILE_MANAGEMENT_EXPERIMENT_NAME)
    with mlflow.start_run(run_name=name):
        mlflow.log_dict(profile, "profile.json")
        mlflow.log_params(_flatten(profile))


def fetch_profile(name: str) -> dict | None:
    """지금 S3에 저장된 프로필 원문을 읽는다(수정 전 확인, `push_profile()`과 짝).

    returns:
        dict | None: 없으면 None(`common_config._load_profile()`은 이 경우 내장
            기본값으로 폴백한다)
    """
    return s3_io.read_json(profile_path(name))


def list_profiles() -> list[str]:
    """S3 `profiles/` 밑에 있는 프로필 이름 목록(확장자 제외, 정렬됨)."""
    prefix = f"{PROFILES_PREFIX}/"
    keys = s3_io.list_keys(prefix)
    return sorted(k.removeprefix(prefix).removesuffix(".json") for k in keys if k.endswith(".json"))
