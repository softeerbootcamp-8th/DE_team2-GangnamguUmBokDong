"""training이 학습하고 inference가 그대로 재현해야 하는 "모델 입력 계약".

**왜 따로 뺐는가**: 학습(`training/train_common.py`)과 서빙(`inference/predict_common.py`,
`inference/predict_single.py`)이 서로 다른 인스턴스에서 돌아가지만, 둘 다 LightGBM에
**정확히 같은 순서·같은 station_no 카테고리 인코딩**으로 feature를 넣어야 한다 —
하나라도 어긋나면 모델이 조용히 엉뚱한 컬럼을 다른 의미로 읽는 사고가 난다. 이
계약(feature 목록 + station_no 카테고리 저장/로드)을 두 인스턴스가 각자 복사해
두면 한쪽만 고치고 잊어버리는 사고가 나므로, `ml_core/`으로 모아 양쪽이 같은
모듈을 import하게 한다.

대여/반납은 이제 완전히 분리된 데이터셋·모델이라(서로 상대방의 lag를 보지
않음), `RENTAL_FEATURE_COLUMNS`/`RETURN_FEATURE_COLUMNS` 두 개로 나눈다 — 공통
캘린더/공간 피처(`BASE_FEATURE_COLUMNS`)에 각자의 lag 컬럼 1개만 붙인다. 실제
값을 계산하는 로직(`feature_engine/spark/build_features.py`)은 여기 없다(그건
feature_engine의 책임) — "스키마 정의"는 `ml_core/`이 소유하고, "그 스키마를
실제로 채우는 계산"은 feature_engine이 소유하는 구조다.

**dtype도 같은 이유로 계약에 포함**: `NATIVE_COLUMN_DTYPES`/`RENTAL_FEATURE_COLUMN_DTYPES`/
`RETURN_FEATURE_COLUMN_DTYPES`는 원래 `feature_engine/spark/build_merged_table.py`에만
있던 다운캐스트(int8/int16/float32)
매핑이었는데, `inference/predict_single.py`가 Python 스칼라로 feature 행을 새로
조립할 때는 이걸 안 써서 학습 데이터(float32 등)와 서빙 입력(float64 기본값)의
dtype이 어긋나 있었다 — 예측값 자체는 달라지지 않지만(LightGBM이 내부적으로
캐스팅) 스키마 불일치 자체가 다른 종류의 조용한 skew를 부를 수 있어 계약으로
옮겼다.
"""

import pandas as pd
from core import s3 as s3_io
from core.model_snapshot import parse_station_categories

from . import common_config
from .paths import MODELS_PREFIX, read_champion_prefix

RENTAL_FEATURE_COLUMNS = [*common_config.BASE_FEATURE_COLUMNS, "rental_lag_1h"]
RETURN_FEATURE_COLUMNS = [*common_config.BASE_FEATURE_COLUMNS, "return_lag_1h"]

# 값 범위 대비 과한 자료형(float64/int64)을 실측 최소~최대 범위에 맞게 줄인 매핑
# (feature_engine/spark/build_merged_table.py에서 이관 — 근거/실측 범위는 그 파일 참고).
# bike_count/stockout_flag/minute처럼 FEATURE_COLUMNS엔 없지만 feature_engine 내부
# 중간 산출물에 쓰이는 컬럼도 포함한다(build_merged_table.py가 그대로 재사용).
# day(2000-01-01 기준 경과일수, ml_core.day_index)/station_no는 int16 — Spark엔
# unsigned 타입이 없어(ShortType이 최대) build_merged_table.py가 이미 부호 있는
# 16비트로 저장한다. pandas 쪽만 uint16으로 선언해도 Spark가 쓴 값 자체가 이미
# int16 범위(32,767)로 잘려 있어 실제로 더 넓은 범위를 못 쓴다 — 그래서 여기도
# 있는 그대로 int16으로 맞춘다(32,767까지, day 기준 약 서기 2089년까지 충분).
NATIVE_COLUMN_DTYPES = {
    "bike_count": "int16",
    "stockout_flag": "int8",
    "rental_count": "int16",
    "return_count": "int16",
    # capacity(거치대 수) — 예전엔 "LEFT JOIN이라 결측 가능"이라 float32였는데,
    # 실제로 이 join은 결측을 낼 수 없다는 걸 코드 경로로 확인했다(2026-08,
    # build_merged_table.py의 NATIVE_COLUMN_DTYPES 주석에 join 체인 근거 전체 기록).
    # 항상 정수(거치대 개수)라 int16으로 줄인다 — station_no/day와 이유가 다름을
    # 표시하기 위해 별도 주석으로 남긴다.
    "capacity": "int16",
    "lat": "float32",
    "lon": "float32",
    "temp": "float32",
    "precip": "float32",
    "pop_total": "float32",
    "hour": "int8",  # 더 이상 모델 feature가 아니다(minute이 대체) — 출력/CLI 식별용으로만 남음
    # minute = 자정 기준 경과분(0~1439, ml_core.minute_of_day) — hour 대신 쓰는 실제
    # 모델 feature. int8(-128~127) 범위를 넘어 int16 필요.
    "minute": "int16",
    "dow": "int8",
    "is_holiday": "int8",
    "horizon": "int8",  # 1~HORIZON_COUNT(기본 12) — common_config.HORIZON_COUNT 참고
    "day": "int16",
    # station_no(station_id 대신 모델이 실제로 쓰는 정류소 카테고리 키) — 카테고리
    # 코드 자체는 load_station_dtype()이 별도 관리한다(아래 _feature_column_dtypes()가
    # station_no는 이 dtype 매핑에서 빼고 카테고리로만 다룬다 — station_id를 빼던
    # 것과 같은 이유).
    "station_no": "int16",
    # int16이 아니라 float32인 이유: 둘 다 결측 가능하다 — 배치(feature_engine)는
    # LEFT JOIN 결과가 그리드 구멍에서 null일 수 있고, 서빙(predict_single.py)은
    # profile fallback마저 실패하면 np.nan을 그대로 채운다. pandas의 plain int16은
    # NaN을 표현할 수 없어 `.astype()`에서 바로 크래시한다(LightGBM은 결측을
    # 네이티브로 처리하므로 float32 그대로 두는 게 맞다).
    #
    # **pandas nullable "Int16"(대문자, 결측 지원)로 다이어트하는 안을 검토했으나
    # 기각했다(2026-08)**: LightGBM에 실제로 태워보면 float32-NaN과 Int16-pd.NA가
    # bit-identical한 예측을 내서 그 자체는 문제 없다 — 그런데 predict_single.py의
    # `_profile_stat()` fallback(`rental_mean`/`return_mean`, station_hourly_profile의
    # 표본 평균)은 실측값 평균이라 **거의 항상 소수**다(예: 3.4). `Series.astype("Int16")`은
    # 그런 소수 float에 대해 `TypeError: cannot safely cast non-equivalent float64 to
    # int16`으로 바로 죽는다(실측 확인 완료) — 배치(feature_engine) 쪽은 fallback 없이
    # 항상 정수-또는-null이라 문제 없지만, 서빙의 fallback 경로에서 그대로 크래시한다.
    # fallback을 반올림해서 정수로 강제하면 가능하지만, 그건 "평균값의 소수 정밀도를
    # 버린다"는 별도의 동작 변경이라 여기서 마음대로 결정하지 않았다 — station_no/day/
    # capacity와 달리 이 둘은 float32로 남긴다.
    "rental_lag_1h": "float32",
    "return_lag_1h": "float32",
}
RENTAL_EXPOSURE_DTYPE = "float32"  # features.py의 rental_exposure와 동일(FEATURE_COLUMNS엔 없음 — init_score offset 전용)


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
