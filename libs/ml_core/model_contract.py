"""학습(training)과 추론(inference) 간에 공유되는 모델 입력 스키마 및 데이터 타입 계약 정의."""

import pandas as pd
from core import s3 as s3_io
from core.model_snapshot import parse_station_categories

from . import common_config
from .paths import MODELS_PREFIX, read_champion_prefix

RENTAL_FEATURE_COLUMNS = [*common_config.BASE_FEATURE_COLUMNS, "rental_lag_1h"]
RETURN_FEATURE_COLUMNS = [*common_config.BASE_FEATURE_COLUMNS, "return_lag_1h"]

# 각 피처 및 메타데이터 컬럼별 네이티브 데이터 타입 매핑
NATIVE_COLUMN_DTYPES = {
    "bike_count": "int16",
    "stockout_flag": "int8",
    "rental_count": "int16",
    "return_count": "int16",
    "capacity": "int16",
    "lat": "float32",
    "lon": "float32",
    "temp": "float32",
    "precip": "float32",
    "pop_total": "float32",
    "hour": "int8",
    "minute": "int16",
    "dow": "int8",
    "is_holiday": "int8",
    "horizon": "int8",
    "day": "int16",
    "station_no": "int16",
    "rental_lag_1h": "float32",
    "return_lag_1h": "float32",
}
RENTAL_EXPOSURE_DTYPE = "float32"



def _feature_column_dtypes(feature_columns: list[str]) -> dict[str, str]:
    """station_no(카테고리 코드는 load_station_dtype()이 별도 관리)를 뺀 나머지
    feature 컬럼의 dtype 매핑을 만든다."""
    return {
        col: NATIVE_COLUMN_DTYPES[col] for col in feature_columns if col != "station_no"
    }


# pandas DataFrame.astype(dict)는 dict에 그 df에 없는 키가 하나라도 있으면
# KeyError를 던진다 — 대여/반납 record가 서로 다른 컬럼 집합을 갖게 됐으므로
# (rental_lag_1h+rental_exposure vs return_lag_1h) 하나로 합친 dtype dict를 쓰면
# 안 되고, 모델별로 정확히 자기 컬럼만 담은 dict를 따로 둬야 한다.
RENTAL_FEATURE_COLUMN_DTYPES = {
    **_feature_column_dtypes(RENTAL_FEATURE_COLUMNS),
    "rental_exposure": RENTAL_EXPOSURE_DTYPE,
}
RETURN_FEATURE_COLUMN_DTYPES = _feature_column_dtypes(RETURN_FEATURE_COLUMNS)


def station_dtype_from_payload(payload: bytes) -> pd.CategoricalDtype:
    """Pinned model snapshot의 canonical category bytes로 dtype을 만든다."""
    categories = parse_station_categories(payload)
    return pd.CategoricalDtype(categories=list(categories))


def station_categories_path(model_name: str, models_prefix: str | None = None) -> str:
    """model_name에 대응하는 station_no 카테고리 목록 json의 S3 키를 반환한다.

    args:
        model_name: "rental" 또는 "return"
        models_prefix: None이면 챔피언 저장 prefix(paths.MODELS_PREFIX) — 실험/스윕
            실행은 자신만의 prefix(예: "models/experiments/{run_id}")를 넘겨서
            챔피언 아티팩트를 덮어쓰지 않는다.
    returns:
        str: "{models_prefix}/{model_name}_station_categories.json"
    """
    return f"{models_prefix or MODELS_PREFIX}/{model_name}_station_categories.json"


def load_station_dtype(
    model_name: str, models_prefix: str | None = None
) -> pd.CategoricalDtype:
    """학습 때 고정한 station_no 카테고리 목록을 그대로 불러온다 (predict에서 코드 어긋남 방지용).

    args:
        model_name: "rental" 또는 "return"
        models_prefix: None이면 "지금 챔피언"의 archive_prefix를
            `read_champion_prefix()`로 구해서 쓴다(그 함수 docstring 참고 —
            `ml_core.scoring`의 `load_boosters()`/`load_conformal_correction()`과
            같은 프로세스 캐시를 공유해서, 한 프로세스 안에서는 booster/보정값/
            station_categories가 항상 같은 archive_prefix에서 나오게 한다).
            명시적으로 주면(실험/스윕 등) 그 값을 그대로 쓴다.
    returns:
        pd.CategoricalDtype: 학습 시점과 동일한 순서의 station_no 카테고리
    raises:
        FileNotFoundError: station_categories가 없을 때, 또는(models_prefix가
            None인데) 아직 한 번도 승격된 적 없을 때
    """
    resolved_prefix = (
        models_prefix if models_prefix is not None else read_champion_prefix(model_name)
    )
    categories = s3_io.read_json(station_categories_path(model_name, resolved_prefix))
    if categories is None:
        raise FileNotFoundError(
            f"station_categories 없음: {station_categories_path(model_name, resolved_prefix)}"
        )
    return pd.CategoricalDtype(categories=categories)
