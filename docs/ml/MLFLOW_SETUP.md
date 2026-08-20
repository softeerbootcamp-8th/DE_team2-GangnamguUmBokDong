# MLflow 실험 추적 — 세팅과 사용법

`training/`이 매 학습(`train_target()`)과 매달 성능 점검(`monitor_performance.py`)
결과를 [MLflow](https://mlflow.org/)에 기록한다. 무엇을 위한 것이고 무엇이
**아닌지**부터 분명히 하자:

- **하는 일**: 학습 하이퍼파라미터·데이터 분할 설정·평가 지표·모델 파일(booster)을
  실행(run)마다 기록해서, 여러 번의 학습 시도(예: `TRAIN_DAY_DIVISOR`/
  `MAX_TRAIN_HORIZON` 조합을 바꿔가며 재시도한 이력)를 웹 UI에서 나란히 비교할 수
  있게 한다.
- **안 하는 일**: 피처를 만들거나 계산하지 않는다. 오픈소스(자체 호스팅) MLflow는
  Tracking/Model Registry/Projects/서빙만 제공하고, "Feature Store"(피처 조회,
  point-in-time join)는 Databricks 관리형 제품(Unity Catalog 종속)에만 있는
  기능이라 이 저장소가 쓰는 `mlflow`/`mlflow-skinny` 패키지엔 없다. 피처는 지금도
  전부 `feature_engine`(Spark)이 만들고, MLflow는 그 이후 학습 결과만 기록한다.
- 챔피언 승격도 대체하지 않는다 — "지금 서빙 중인 모델이 어느 archive인지"는
  여전히 `ml_core.paths`의 챔피언 포인터(`docs/ml/training/DESIGN.md` 참고)가
  결정한다. MLflow는 그 결정에 참고할 지표를 보여주는 대시보드일 뿐이다.

## 아키텍처

```
training(로컬/EMR) ──HTTP──> mlflow 서버(ops/compose) ──JDBC──> postgres(mlflow DB, 메타데이터)
                                      │
                                      └──S3 API──> minio(mlflow-artifacts/, 아티팩트 실체)
```

`ops/compose/docker-compose.yml`의 `mlflow` 서비스가 `--serve-artifacts`로 뜬다 —
서버가 S3(MinIO) 업로드/다운로드를 대신 중계하므로, **클라이언트(학습 코드)는
S3 자격증명을 몰라도 된다.** `MLFLOW_TRACKING_URI` 환경변수 하나만 맞으면
로컬이든 EMR이든 어디서 실행되는 학습 프로세스든 같은 서버에 기록된다 —
`S3_ENDPOINT_URL`/`DATABASE_URL`을 세 인스턴스(feature_engine/training/inference)가
공유하는 것과 정확히 같은 패턴이다.

## 로컬에서 띄우기

```bash
# 저장소 루트에서
make up          # postgres/minio/mlflow/airflow 전부 기동 (ops/compose/docker-compose.yml)
# 또는 mlflow만 다시 띄우고 싶을 때
docker compose --env-file .env -f ops/compose/docker-compose.yml up -d --build mlflow
```

기동하면 웹 UI는 `http://localhost:5000`(`.env`의 `MLFLOW_PORT`, 기본 5000)에서
열린다. Backend store는 기존 `postgres` 서비스의 `mlflow` DB
(`POSTGRES_MLFLOW_DB`), artifact store는 기존 `minio` 버킷의
`mlflow-artifacts/` prefix — 별도 인프라를 새로 안 띄우고 이미 있는
postgres/minio를 재사용한다.

**postgres 볼륨이 이미 만들어져 있던 환경이라면** `mlflow` DB가 자동으로
안 생긴다(`ops/postgres/init/*.sh`는 최초 볼륨 생성 시에만 실행됨) — 그럴 때만
한 번:

```bash
docker exec <postgres-container-이름> createdb -U postgres mlflow
```

## 환경변수 (.env)

| 변수 | 기본값 | 설명 |
|---|---|---|
| `POSTGRES_MLFLOW_DB` | `mlflow` | backend store로 쓸 postgres DB 이름 |
| `MLFLOW_PORT` | `5000` | 웹 UI/API 포트 |
| `MLFLOW_TRACKING_URI` | `http://localhost:${MLFLOW_PORT}` | 클라이언트가 붙을 서버 주소 — 컨테이너 밖(로컬 프로세스)은 `localhost`, 컨테이너 안에서 실행되는 코드는 `http://mlflow:5000`으로 바꿔줘야 함 |
| `MLFLOW_EXPERIMENT_NAME` | `bike-demand-training` | `train_target()`이 쓰는 experiment (`ml/training/config.py`) |
| `MLFLOW_MONITORING_EXPERIMENT_NAME` | `bike-demand-monitoring` | `monitor_performance.py`가 쓰는 별도 experiment — 학습 run과 섞이면 "이번 달 드리프트 추이"를 보기 번거로워서 분리 |

클라이언트 쪽 코드는 `libs/ml_core/mlflow_tracking.py`의 `configure(experiment_name)`
하나만 거친다 — `mlflow.set_tracking_uri()`/`mlflow.set_experiment()`를 대신
불러준다.

## 무엇이 기록되는가

### 학습 run (`train_common.train_target()`, experiment: `bike-demand-training`)

분산 학습(`LGB_NUM_MACHINES>1`) 시 대표 머신(rank 0)만 run을 연다. run 하나 =
`train_rental_model`/`train_return_model` 실행 1회.

- **run 이름**: `{model_name}_{profile_name}_d{TRAIN_DAY_DIVISOR}_h{MAX_TRAIN_HORIZON}`
  (예: `rental_default_d2_h6`) — 날짜가 아니라 실제로 실험마다 바뀌는 값(day
  divisor/horizon 상한)을 이름에 담아서, 같은 날 여러 조합을 재시도해도 목록에서
  구분이 된다.
- **params**: `train_year`/`train_day_divisor`/`max_train_horizon`/
  `valid_days_of_month`/`test_days_of_month`/`*_sample_frac`/`profile_name`/
  `models_prefix`/`train_rows`/`valid_rows`/`test_rows`/`feature_columns`
  (콤마로 이어붙인 문자열)/`lgb_num_boost_round`/`lgb_early_stopping_rounds` +
  `LGB_PARAMS_COMMON`의 모든 하이퍼파라미터(`num_leaves`/`learning_rate`/
  `feature_fraction`/`bagging_fraction`/`bagging_freq`/`min_data_in_leaf` 등).
- **metrics**: `train_target()`이 반환하는 것과 동일 — `poisson_deviance_test`,
  `rmse_test`, `best_iteration`, `pinball_test_q{10,50,90}`,
  `p10_p90_coverage_raw_test`, `conformal_correction`,
  `p10_p90_coverage_calibrated_test`.
- **artifacts**(`models/` 아래): booster 4개(`rental_poisson.txt` 등, 실제 S3 키
  이름 그대로) + `station_categories.json` + `conformal_correction.json` +
  `profile.json`(프로필 전체 설정 — embargo/tick 등 서빙 재현에 필요한 값).
  전부 S3 아카이브에도 동시에 저장되며 MLflow가 그 자리를 대체하진 않는다 —
  "이 run이 정확히 어떤 바이트를 냈는지" 웹 UI에서 바로 열어보는 보조 사본이다.
- run은 `with mlflow.start_run(...)`로 열려서, 학습 도중 예외가 나도 RUNNING
  상태로 방치되지 않고 자동으로 FAILED 처리된다.

### 월별 점검 run (`monitor_performance.py`, experiment: `bike-demand-monitoring`)

`check_all_models()`가 대여/반납마다 점검 1회 = run 1개.

- **run 이름**: `{model_name}_h{horizon}_{period_end}` (예: `rental_h1_2026-07-31`)
- **params**: `model_name`/`horizon`/`period_start`/`period_end`
- **metrics**: `n_rows`, `baseline_deviance`, `current_deviance`,
  `deviance_relative_change`, `baseline_rmse`, `current_rmse`,
  `baseline_coverage`, `current_coverage`, `coverage_drift`,
  `needs_retrain`(0/1)
- 재학습 사유가 있으면(`reasons`) `reasons.json` 아티팩트로 남는다.
- **MLflow 로깅 자체가 실패해도(서버가 안 떠 있는 등) 재학습 필요 여부 판단은
  안 막힌다** — 예외를 삼키고 경고만 출력한다(`monitor_performance._log_to_mlflow()`).
  즉 이 서버는 순수 관측용이지, 없으면 파이프라인이 죽는 필수 컴포넌트가 아니다.

## UI에서 실험 비교하기

`http://localhost:5000` → 왼쪽에서 experiment(`bike-demand-training` 등) 선택 →
run 목록 테이블에서 컬럼 추가(⚙️ 아이콘)로 비교하고 싶은 param/metric을 켠다.
예: `train_day_divisor`, `max_train_horizon`, `poisson_deviance_test`,
`p10_p90_coverage_calibrated_test` 컬럼을 켜두면 "divisor를 3→5로 올렸을 때
성능이 얼마나 떨어졌는지"를 표로 바로 비교할 수 있다. 정렬(컬럼 헤더 클릭)로
가장 좋은 deviance를 낸 run을 바로 찾을 수도 있다.

## 로컬에서 직접 mlflow를 호출해볼 때 (스모크 테스트)

```bash
cd ml/training
set -a && source ../../.env && set +a
./.venv/bin/python -c "
from ml_core import mlflow_tracking
import mlflow

mlflow_tracking.configure('smoke-test')
with mlflow.start_run(run_name='smoke') as run:
    mlflow.log_params({'foo': 'bar'})
    mlflow.log_metrics({'rmse': 1.23})
    print('run_id:', run.info.run_id)
"
```

`http://localhost:5000`에서 `smoke-test` experiment에 run이 보이면 정상 배선된
것 — 확인 후엔 MLflow UI에서 그 experiment를 지우거나
`MlflowClient().delete_experiment(...)`로 정리할 것(스모크 테스트용 잡음이라
실제 지표와 섞이면 안 됨).

## 테스트에서 MLflow 쓰기

`ml/training/tests/conftest.py`의 `_no_real_mlflow_server` 오토유즈 fixture가
`MLFLOW_TRACKING_URI`를 존재하지 않는 포트(`http://localhost:0`)로 돌려막아서,
아무 설정 없이 테스트를 돌려도 로컬에 떠 있는 진짜 mlflow 서버에 실수로 run이
쌓이지 않는다. 실제 MLflow 동작 자체를 검증하는 테스트
(`dev_train_target_mlflow.py`, `dev_monitor_performance.py`의 `_log_to_mlflow`
테스트)는 자기 fixture에서 로컬 파일 backend(`tmp_path`)로 다시 덮어써서 쓴다 —
MLflow 3.x는 파일 backend를 기본으로 막아서(`MLFLOW_ALLOW_FILE_STORE=true`
환경변수로 opt-out) sqlalchemy 없이도 테스트가 가능하다(`mlflow-skinny`는
sqlite backend에 필요한 sqlalchemy를 일부러 안 갖고 있음).

## 관련 문서

- [training/DESIGN.md](training/DESIGN.md) — 학습 로직 전체 설계(챔피언 승격 등)
- `libs/ml_core/mlflow_tracking.py` — 클라이언트 진입점 소스
- `ops/compose/docker-compose.yml`의 `mlflow` 서비스 정의, `ops/compose/Dockerfile.mlflow`
