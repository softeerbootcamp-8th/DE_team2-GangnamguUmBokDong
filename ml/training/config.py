"""training(LightGBM 학습) 전용 경로·상수.

`feature_engine`이 만든 multi-horizon feature 테이블을 S3에서 읽어
(`RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET`/`RETURN_..._PARQUET`, `ml_core/`에서
공유) `MODELS_PREFIX`(S3 키 prefix, `models/`)에 학습 결과를 저장한다. `inference`도
같은 `MODELS_PREFIX`를 읽어야 하므로 그 값 자체는 `ml_core/paths.py`가 소유하고,
여기서는 학습에만 쓰는 값(split 기준, quantile 목록 등)을 정의한다.
"""

import os
import uuid
from datetime import date, timedelta
from math import ceil

from ml_core import common_config
from ml_core.paths import (
    MODELS_PREFIX,
    PROCESSED_V2_PREFIX,
    RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
    RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
)

__all__ = [
    "MODELS_PREFIX",
    "PROCESSED_V2_PREFIX",
    "RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET",
    "today_kst",
    "unique_archive_date",
]

# KST 기준 "오늘"은 이제 common_config.py 소유다(feature_engine도 학습기간 롤링
# 윈도우 계산에 필요해져서 공유 위치로 옮김) — 여기는 하위 호환을 위해 그대로 재수출.
today_kst = common_config.today_kst


def unique_archive_date(as_of: date | None = None) -> str:
    """실행마다 유일한 아카이브 날짜 문자열("{오늘}-{실행 유일 접미사}")을 만든다.

    `ml_core.paths.archive_models_prefix()`는 date+profile_name만으로 경로를
    만들기 때문에, 같은 날 같은 프로필로 학습을 두 번 실행하면(수동 재실행, 다른
    오케스트레이터의 재시도 등) archive_prefix가 겹쳐 이미 챔피언이 가리키는
    아티팩트를 비원자적으로 덮어쓸 수 있다(리뷰 지적 — archive는 immutable이어야
    한다는 설계가 무력화됨). 이 유일성은 "학습 한 번의 시도" 단위로 호출부가
    한 번만 계산해서 archive_models_prefix()에 넘겨야 한다 — 그 함수 자체 안에서
    매번 새 값을 생성하면, 같은 시도 안에서 이 함수를 여러 번 호출하는 곳
    (`monthly_retrain_check.py`가 학습 시작 시/승격 판단 시 두 번 호출)이 서로
    다른 prefix를 보게 돼 깨진다. `MODEL_ARCHIVE_DATE`가 명시되지 않은 모든 학습
    진입점(train_rental_model.py/train_return_model.py/train_common.py의 ad-hoc
    블록/monthly_retrain_check.py)이 이 함수를 기본값으로 써서, 어느 경로로
    실행되든 매 실행이 항상 새 archive_prefix를 가리키게 한다.

    args:
        as_of: 기준 날짜(기본 오늘) — 테스트에서 날짜를 고정하기 위한 override
    returns:
        str: "YYYY-MM-DD-{8자리 hex}"
    """
    return f"{(as_of or today_kst()).isoformat()}-{uuid.uuid4().hex[:8]}"

# --- 학습/검증/평가 split (day-of-month 기준) ---
# 예전엔 TRAIN/VALID/TEST를 시간 순(walk-forward)으로 연속 구간(20/5/5일)만 뽑아
# 썼다 — multi-horizon 테이블이 원본의 최대 HORIZON_COUNT배 행 수라 단일 머신
# LightGBM이 1년 전체를 못 받았기 때문(history.md 18번 항목).
#
# **2026-08 실측 + 이후 정책 변경**: 대여/반납 분리 + lag 1개로 줄인 뒤에도(피처
# 축소 이후) 당시 20분 tick·1년 전체 multi-horizon 테이블이 8억 행이라 로컬
# (RAM 18GB)에서 pandas로 한 번에 못 읽어서(에러 메시지도 없이 SIGKILL), 한때
# day-of-month 배수로 날짜 자체를 줄이는(`TRAIN_DAY_DIVISOR`, 기본 2=짝수날만)
# 임시 조치를 도입했었다. 이후 **학습·추론 모두 5분 tick 앵커 밀도를 유지**하는
# 운영 계약이 확정되면서(minute 단위 서빙 요청을 실제로 커버해야 함 — 시간
# 단위로 앵커를 줄이면 모델이 그 minute 값들을 아예 학습에서 못 봄), 날짜를
# 솎아내는 방식으로 메모리를 줄이는 건 기본값에서 뺐다** — `TRAIN_DAY_DIVISOR`
# 기본값을 다시 1(=날짜 필터 없음, 전체 윈도우 사용)로 되돌렸다. 대신 실제 메모리
# 문제는 `lazy_train_dataset.py`의 날짜 파티션 단위 스트리밍 학습으로 푼다 —
# 그게 어려운 특수 상황(예: 로컬에서 급하게 뭔가 검증)에서만 `TRAIN_DAY_DIVISOR`를
# 다시 2, 3, 5로 올리는 임시 dial로 남겨둔다.
#
# train은 **`TRAIN_DAY_DIVISOR`의 배수인 날 중 VALID/TEST 및 그 embargo 구간으로
# 안 뽑힌 날**
# (기본 1 = 사실상 전체 날짜), valid/test는 `VALID_DAYS_OF_MONTH`/
# `TEST_DAYS_OF_MONTH`로 지정한 날짜만 쓴다 — `train_common._dates_for_split()`이
# valid/test 여부를 먼저 확정하고 그 앞뒤 `SPLIT_EMBARGO_DAYS`까지 purge한 뒤
# train 배수 조건을 보므로, TRAIN_DAY_DIVISOR가 1이라 "모든 날짜가 배수"인
# 경우에도 평가 anchor가 train으로 새지 않는다.
TRAIN_DAY_DIVISOR = int(os.environ.get("TRAIN_DAY_DIVISOR", "1"))


def _day_set_env(name: str, default: str) -> frozenset[int]:
    """쉼표로 구분한 학습 split 날짜 환경변수를 검증해 반환한다."""
    try:
        days = frozenset(int(value.strip()) for value in os.environ.get(name, default).split(",") if value.strip())
    except ValueError as exc:
        raise ValueError(f"{name}은 쉼표로 구분한 정수여야 합니다") from exc
    if not days or any(day < 1 or day > 31 for day in days):
        raise ValueError(f"{name}은 1~31 사이 날짜를 하나 이상 포함해야 합니다: {sorted(days)}")
    return days


VALID_DAYS_OF_MONTH = _day_set_env("VALID_DAYS_OF_MONTH", "11,13")
TEST_DAYS_OF_MONTH = _day_set_env("TEST_DAYS_OF_MONTH", "17,19")
if VALID_DAYS_OF_MONTH & TEST_DAYS_OF_MONTH:
    raise ValueError(
        "VALID_DAYS_OF_MONTH와 TEST_DAYS_OF_MONTH는 겹칠 수 없습니다: "
        f"{sorted(VALID_DAYS_OF_MONTH & TEST_DAYS_OF_MONTH)}"
    )

# --- 학습기간 윈도우 (고정 최초학습 또는 rolling 재학습) ---
# `common_config.training_window()`는 TRAIN_WINDOW_START/END 쌍이 있으면 그 exact
# 구간을, 없으면 프로필의 TRAIN_LOOKBACK_MONTHS/TRAINING_SAFETY_MARGIN_DAYS로
# "오늘 기준 최근 N개월"을 계산한다. feature_engine/spark/config.py와 동일 함수를
# 공유하므로 두 단계가 다른 범위를 보는 것을 막는다. 월별 재학습은 고정 구간
# 변수를 제거하고 rolling 값을 매번 새로 계산해 최신 데이터까지 포함한다.
#
# TRAINING_SAFETY_MARGIN_DAYS는 대여이력이 반납 완료 시에만 Silver에 나타나는
# 것과 같은 이유로(feature_engine/spark/run_pipeline.py의 날짜 파티션 overwrite
# 보정 참고) 최근 며칠은 라벨이 아직 안 굳었을 수 있어 학습/평가에서 뺀다 — 예전엔
# 이 파일 전용 환경변수였는데, feature_engine도 같은 마진이 필요해져서
# common_config로 승격됐다(값은 여기서 재수출해 기존 `config.TRAINING_SAFETY_MARGIN_DAYS`
# 참조/monkeypatch 하위 호환을 유지).
TRAINING_SAFETY_MARGIN_DAYS = common_config.TRAINING_SAFETY_MARGIN_DAYS
TRAIN_WINDOW_START, TRAIN_WINDOW_END = common_config.training_window()


def safety_cutoff_date(as_of: date | None = None) -> date:
    """`as_of - TRAINING_SAFETY_MARGIN_DAYS` — 이 날짜를 넘는 라벨은 아직 확정되지
    않았을 수 있어 학습/평가에 쓰지 않는다.

    `monitor_performance.py`가 여전히 이 함수를 쓴다(월별 실측 성능 구간 계산,
    학습기간 자체와는 다른 용도) — `TRAIN_WINDOW_END`는 이미 이 마진이 적용된
    값이라 `train_common.py`는 이 함수를 직접 부르지 않고 `TRAIN_WINDOW_END`를
    그대로 쓴다.

    args:
        as_of: 기준 날짜(기본 오늘) — 테스트에서 날짜를 고정하기 위한 override
    """
    as_of = as_of or today_kst()
    return as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


QUANTILE_ALPHAS = [0.1, 0.5, 0.9]


def _reject_unsupported_sample_frac_env() -> None:
    """구현되지 않은 row-fraction 샘플링 환경변수를 명시적으로 거부한다.

    과거 `TRAIN/VALID/TEST_SAMPLE_FRAC` 상수는 실제 데이터 로더 어느 곳에서도
    적용되지 않아 값을 낮춰도 전 행을 읽는 가짜 dial이었다. OOM 폴백은 실제로
    작동하는 `TRAIN_DAY_DIVISOR`와 `MAX_TRAIN_HORIZON`만 사용한다.
    """
    names = ("TRAIN_SAMPLE_FRAC", "VALID_SAMPLE_FRAC", "TEST_SAMPLE_FRAC")
    configured = sorted(name for name in names if name in os.environ)
    if configured:
        raise ValueError(
            f"지원하지 않는 샘플링 환경변수입니다: {configured}. "
            "TRAIN_DAY_DIVISOR 또는 MAX_TRAIN_HORIZON을 사용하세요"
        )


_reject_unsupported_sample_frac_env()

# **2026-08**: 같은 날짜 파티션 안에 horizon 1~HORIZON_COUNT이 전부 섞여 있어
# 날짜 필터만으론 못 줄이는 배율이라, 필요하면 읽는 시점에
# `horizon <= MAX_TRAIN_HORIZON` 필터를 걸(core.s3.read_parquet의 filters=,
# row-group 단위로 걸러져서 날짜 필터와 같은 원리로 로드 자체를 줄인다) 수
# 있게 남겨둔 dial이다. **기본값은 제한 없음(HORIZON_COUNT=12 그대로, 즉 항상
# 전체 horizon으로 학습)** — horizon도 낮추지 않는 게 확정된 정책이라(위
# TRAIN_DAY_DIVISOR 주석 참고), OOM 대응은 이 값을 낮추는 대신 실제
# 스트리밍/분산 학습 쪽으로 푼다. 로컬에서 급하게 뭔가 검증할 때만 임시로
# 낮출 것 — 낮추면 그 이상 horizon에 대한 예측 품질은 검증되지 않는다(모델이
# 그 구간의 실제 예를 아예 못 봄).
MAX_TRAIN_HORIZON = int(os.environ.get("MAX_TRAIN_HORIZON", str(common_config.HORIZON_COUNT)))
if not 1 <= MAX_TRAIN_HORIZON <= common_config.HORIZON_COUNT:
    raise ValueError(
        f"MAX_TRAIN_HORIZON은 1~{common_config.HORIZON_COUNT} 사이여야 합니다: {MAX_TRAIN_HORIZON}"
    )

# multi-horizon 한 anchor는 target 날짜가 자정을 넘으면 서로 다른 `date=` 파티션에
# 최대 MAX_TRAIN_HORIZON개 행으로 나뉜다. train/valid/test를 day-of-month로만
# 인터리브하면 같은 anchor의 거의 같은 입력이 train과 평가셋에 동시에 들어가므로,
# horizon 이동 폭과 target 집계 창을 모두 덮는 날짜 단위 purge 구간을 둔다.
# 현재 12시간 예측·60분 target이면 1일이며, horizon을 늘리면 자동으로 커진다.
_MIN_SPLIT_EMBARGO_DAYS = ceil(
    (((MAX_TRAIN_HORIZON - 1) * 60) + common_config.TARGET_HORIZON_MINUTES) / (24 * 60)
)
SPLIT_EMBARGO_DAYS = int(os.environ.get("SPLIT_EMBARGO_DAYS", str(_MIN_SPLIT_EMBARGO_DAYS)))
if SPLIT_EMBARGO_DAYS < _MIN_SPLIT_EMBARGO_DAYS:
    raise ValueError(
        f"SPLIT_EMBARGO_DAYS={SPLIT_EMBARGO_DAYS}는 horizon/target 기준 최소값 "
        f"{_MIN_SPLIT_EMBARGO_DAYS}보다 작을 수 없습니다"
    )

# **2026-08**: divisor=2+horizon<=6로 줄여도 로드가 8시간 넘게 걸리다 디스크
# 스와핑(STAT=U, %CPU 급락)으로 판단해 강제 종료한 사건 이후 도입 — 그 전까지는
# 로드가 실제로 진행 중인지 멈춘 것인지 `ps`의 경과시간을 수동으로 재확인하는 것
# 말고는 알 방법이 없었다. `train_common`/`lazy_train_dataset`이 S3 파일이나 날짜
# 청크를 읽을 때마다 완료 개수(또는 청크)와 그 시점까지의 peak RSS(MB)를 이 파일에
# 이어쓴다(표준출력과 별개 — 표준출력이 다른 곳으로 리다이렉트/버퍼링돼도 이 파일만
# tail 하면 진행 상황을 확인할 수 있다).
TRAIN_PROGRESS_LOG_PATH = os.environ.get("TRAIN_PROGRESS_LOG_PATH", "training_progress.log")
TRAIN_PROGRESS_LOG_INTERVAL_SECONDS = float(os.environ.get("TRAIN_PROGRESS_LOG_INTERVAL_SECONDS", "5"))

# MLflow(ops/compose의 mlflow 서비스, ml_core.mlflow_tracking이 접속을 담당)에
# 이 실험 이름으로 run을 남긴다 — divisor/horizon 조합을 바꿔가며 여러 번 학습을
# 시도할 때(2026-08 OOM 대응 이력) 같은 실험 아래 run들을 나란히 비교하기 위함.
MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME", "bike-demand-training")
# monitor_performance.py의 월별 성능 점검 결과 — 학습 run과 섞이면 MLflow UI에서
# "이번 달 드리프트 추이"를 보기 번거로워져 별도 experiment로 분리한다.
MLFLOW_MONITORING_EXPERIMENT_NAME = os.environ.get("MLFLOW_MONITORING_EXPERIMENT_NAME", "bike-demand-monitoring")

CATEGORICAL_FEATURES = ["station_no"]

# LightGBM 하이퍼파라미터 (common_config.py에서 공유 — feature_engine/spark의 SynapseML
# 학습도 참고할 수 있게)
LGB_PARAMS_COMMON = common_config.LGB_PARAMS_COMMON
LGB_NUM_BOOST_ROUND = common_config.LGB_NUM_BOOST_ROUND
LGB_EARLY_STOPPING_ROUNDS = common_config.LGB_EARLY_STOPPING_ROUNDS

# P10~P90 목표 커버리지 (conformal 보정 기준)
CONFORMAL_TARGET_COVERAGE = common_config.CONFORMAL_TARGET_COVERAGE

# --- 월별 성능 모니터링 / 재학습 트리거 (monitor_performance.py) — common_config.py에서
# 근거와 함께 정의됨(계절성 때문에 절대 임계값 대신 baseline 대비 상대 악화율을 씀) ---
PERFORMANCE_DEGRADATION_THRESHOLD = common_config.PERFORMANCE_DEGRADATION_THRESHOLD
COVERAGE_DRIFT_THRESHOLD = common_config.COVERAGE_DRIFT_THRESHOLD
MONITOR_LOOKBACK_MONTHS = common_config.MONITOR_LOOKBACK_MONTHS

# --- LightGBM 분산 학습 (Socket 기반) — training.DESIGN.md §1 참고 ---
# 워커 IP/포트 등 인프라는 배포 환경마다 다르므로 프로필 파일이 아니라 환경변수로만
# 설정한다. 기본값(tree_learner="serial")은 지금까지와 동일한 단일 머신 로컬 학습이라
# 인프라가 준비되기 전까지 기존 동작을 그대로 유지한다.
LGB_TREE_LEARNER = os.environ.get("LGB_TREE_LEARNER", "serial")  # serial | data | voting | feature
LGB_NUM_MACHINES = int(os.environ.get("LGB_NUM_MACHINES", "1"))
LGB_MACHINE_RANK = int(os.environ.get("LGB_MACHINE_RANK", "0"))  # 0-based, 이 프로세스가 몇 번째 머신인지
# "host:port,host:port,..." 형식 — machine_list_filename 대신 이 문자열을 쓰면 워커마다
# 파일을 따로 배포할 필요가 없다.
LGB_MACHINES = os.environ.get("LGB_MACHINES")
LGB_LOCAL_LISTEN_PORT = int(os.environ.get("LGB_LOCAL_LISTEN_PORT", "12400"))
LGB_TIME_OUT = int(os.environ.get("LGB_TIME_OUT", "120"))  # 분 단위, 다른 머신 연결 대기 타임아웃
