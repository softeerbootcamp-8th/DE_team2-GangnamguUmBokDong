"""training이 학습하고 inference가 그대로 재현해야 하는 "모델 입력 계약".

**왜 따로 뺐는가**: 학습(`training/train_common.py`)과 서빙(`inference/predict_common.py`,
`inference/predict_single.py`)이 서로 다른 인스턴스에서 돌아가지만, 둘 다 LightGBM에
**정확히 같은 순서·같은 station_id 카테고리 인코딩**으로 feature를 넣어야 한다 —
하나라도 어긋나면 모델이 조용히 엉뚱한 컬럼을 다른 의미로 읽는 사고가 난다. 이
계약(feature 목록 + station_id 카테고리 저장/로드)을 두 인스턴스가 각자 복사해
두면 한쪽만 고치고 잊어버리는 사고가 나므로, `ml_core/`으로 모아 양쪽이 같은
모듈을 import하게 한다.

`LAG_ROLLING_FEATURE_COLUMNS`는 `common_config.LAG_HOURS`/`ROLLING_WINDOWS`에서
결정적으로 유도되는 이름 목록이다 — 실제 값을 계산하는 로직(`feature_engine/spark/build_features.py`)은
여기 없다(그건 feature_engine의 책임). `feature_engine/spark/build_features.py`는 이 상수를 그대로
import해서 자기가 만드는 컬럼 이름이 이 계약과 어긋나지 않게 맞춘다 — 즉 "스키마
정의"는 `ml_core/`이 소유하고, "그 스키마를 실제로 채우는 계산"은 feature_engine이
소유하는 구조다.

**dtype도 같은 이유로 계약에 포함**: `NATIVE_COLUMN_DTYPES`/`FEATURE_COLUMN_DTYPES`는
원래 `feature_engine/spark/build_merged_table.py`에만 있던 다운캐스트(int8/int16/float32)
매핑이었는데, `inference/predict_single.py`가 Python 스칼라로 feature 행을 새로
조립할 때는 이걸 안 써서 학습 데이터(float32 등)와 서빙 입력(float64 기본값)의
dtype이 어긋나 있었다 — 예측값 자체는 달라지지 않지만(LightGBM이 내부적으로
캐스팅) 스키마 불일치 자체가 다른 종류의 조용한 skew를 부를 수 있어 계약으로
옮겼다.
"""

import pandas as pd

from . import common_config
from core import s3 as s3_io
from .paths import MODELS_PREFIX

LAG_ROLLING_FEATURE_COLUMNS = [
    f"{prefix}_{suffix}"
    for prefix in ("rental", "return")
    for suffix in (
        [f"lag_{lag}h" for lag in common_config.LAG_HOURS]
        + [f"roll_mean_{w}h" for w in common_config.ROLLING_WINDOWS]
        + [f"roll_std_{w}h" for w in common_config.ROLLING_WINDOWS]
    )
]

FEATURE_COLUMNS = common_config.BASE_FEATURE_COLUMNS + LAG_ROLLING_FEATURE_COLUMNS

# 값 범위 대비 과한 자료형(float64/int64)을 실측 최소~최대 범위에 맞게 줄인 매핑
# (feature_engine/spark/build_merged_table.py에서 이관 — 근거/실측 범위는 그 파일 참고).
# bike_count/stockout_flag/minute처럼 FEATURE_COLUMNS엔 없지만 feature_engine 내부
# 중간 산출물에 쓰이는 컬럼도 포함한다(build_merged_table.py가 그대로 재사용).
NATIVE_COLUMN_DTYPES = {
    "bike_count": "int16",
    "stockout_flag": "int8",
    "rental_count": "int16",
    "return_count": "int16",
    "capacity": "float32",
    "lat": "float32",
    "lon": "float32",
    "temp": "float32",
    "precip": "float32",
    "wind": "float32",
    "humidity": "int8",
    "pop_resd": "float32",
    "pop_long_foreign": "float32",
    "pop_short_foreign": "float32",
    "pop_total": "float32",
    "hour": "int8",
    "minute": "int8",
    "dow": "int8",
    "month": "int8",
    "is_holiday": "int8",
    "is_weekend": "int8",
    "is_next_day_off": "int8",
    "is_prev_day_off": "int8",
    "horizon": "int8",  # 1~HORIZON_COUNT(기본 12) — common_config.HORIZON_COUNT 참고
}

# hour_sin/cos, dow_sin/cos는 build_merged_table.py가 아니라 features.py가 나중에
# 계산해 붙이는 컬럼이라 NATIVE_COLUMN_DTYPES에 없다 — 값이 항상 [-1,1]이라
# float32로 충분(features.py가 이미 이렇게 저장함).
_CYCLICAL_FEATURE_DTYPES = {"hour_sin": "float32", "hour_cos": "float32", "dow_sin": "float32", "dow_cos": "float32"}

# FEATURE_COLUMNS 전체(station_id 제외 — 그건 모델별 카테고리 코드가 따로 필요해
# load_station_dtype()이 담당)의 dtype 계약. lag/rolling은 전부 features.py가
# float32로 만든다.
FEATURE_COLUMN_DTYPES = {
    col: (
        NATIVE_COLUMN_DTYPES[col] if col in NATIVE_COLUMN_DTYPES
        else _CYCLICAL_FEATURE_DTYPES.get(col, "float32")  # 나머지는 전부 LAG_ROLLING_FEATURE_COLUMNS
    )
    for col in FEATURE_COLUMNS
    if col != "station_id"
}
RENTAL_EXPOSURE_DTYPE = "float32"  # features.py의 rental_exposure와 동일 (FEATURE_COLUMNS엔 없음 — init_score offset 전용)


def station_categories_path(model_name: str, models_prefix: str | None = None) -> str:
    """model_name에 대응하는 station_id 카테고리 목록 json의 S3 키를 반환한다.

    args:
        model_name: "rental" 또는 "return"
        models_prefix: None이면 챔피언 저장 prefix(paths.MODELS_PREFIX) — 실험/스윕
            실행은 자신만의 prefix(예: "models/experiments/{run_id}")를 넘겨서
            챔피언 아티팩트를 덮어쓰지 않는다.
    returns:
        str: "{models_prefix}/{model_name}_station_categories.json"
    """
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_station_categories.json"


def load_station_dtype(model_name: str, models_prefix: str | None = None) -> pd.CategoricalDtype:
    """학습 때 고정한 station_id 카테고리 목록을 그대로 불러온다 (predict에서 코드 어긋남 방지용).

    args:
        model_name: "rental" 또는 "return"
        models_prefix: station_categories_path() 참고
    returns:
        pd.CategoricalDtype: 학습 시점과 동일한 순서의 station_id 카테고리
    """
    categories = s3_io.read_json(station_categories_path(model_name, models_prefix))
    if categories is None:
        raise FileNotFoundError(f"station_categories 없음: {station_categories_path(model_name, models_prefix)}")
    return pd.CategoricalDtype(categories=categories)
