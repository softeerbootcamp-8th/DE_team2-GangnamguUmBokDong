# MLflow 실험 추적 설정

> 현재 상태: **Compose와 ML 코드 기준**
>
> 로컬 UI 기본 주소: `http://localhost:5000`

MLflow는 학습·모니터링·프로필 변경 이력을 비교하기 위한 tracking 시스템이다.
Feature Engine의 피처 저장소, S3 model archive, champion pointer 또는 pair serving
release를 대체하지 않는다.

## 1. 역할과 경계

| 시스템 | 책임 |
|---|---|
| Feature Engine | point-in-time feature mart 생성 |
| S3 model archive | 학습 artifact 원본 보관 |
| Serving release | 운영 rental/return pair 결정 |
| MLflow | params·metrics·진행 상태·artifact 사본의 실험 이력 |

MLflow UI에서 좋은 run을 찾았다고 운영 모델이 바뀌지는 않는다. 검수한 model pair를
`training.publish_serving_release`로 게시해야 실제 inference authority가 변경된다.

## 2. 아키텍처

```text
training / monitoring / profile CLI
                 │ HTTP
                 ▼
          MLflow tracking server
            │             │
            │ metadata    │ artifact proxy
            ▼             ▼
      PostgreSQL       S3 / MinIO
       mlflow DB      mlflow-artifacts/
```

로컬 `ops/compose/docker-compose.yml`과 운영
`ops/compose/docker-compose.prod.yml` 모두 `--serve-artifacts`를 사용한다. artifact
업로드는 MLflow 서버가 중계하므로 클라이언트는 `MLFLOW_TRACKING_URI`만 알면 된다.
서버 자체는 로컬에서 MinIO 자격증명을, AWS에서 instance profile을 사용한다.

## 3. 로컬 실행

저장소 루트에서 전체 개발 stack을 시작한다.

```bash
make up
```

MLflow만 다시 빌드·시작하려면 다음을 사용한다.

```bash
docker compose --env-file .env \
  -f ops/compose/docker-compose.yml \
  up -d --build mlflow
```

상태와 로그를 확인한다.

```bash
docker compose --env-file .env \
  -f ops/compose/docker-compose.yml ps mlflow

docker compose --env-file .env \
  -f ops/compose/docker-compose.yml logs mlflow
```

새 PostgreSQL volume은 `ops/postgres/init/001_create_databases.sh`가 `mlflow` DB를
만든다. 이 init script가 추가되기 전에 생성한 기존 volume에서 DB가 없다면 한 번만
생성한다.

```bash
docker compose --env-file .env \
  -f ops/compose/docker-compose.yml \
  exec postgres createdb -U "${POSTGRES_USER:-postgres}" "${POSTGRES_MLFLOW_DB:-mlflow}"
```

이미 DB가 있으면 `createdb`가 실패하므로 먼저 존재 여부를 확인하거나 해당 오류를
그냥 초기화 신호로 사용하지 않는다.

## 4. 환경변수

| 변수 | 로컬 기본값 | 설명 |
|---|---|---|
| `POSTGRES_MLFLOW_DB` | `mlflow` | backend metadata DB |
| `MLFLOW_PORT` | `5000` | host UI/API 포트 |
| `MLFLOW_TRACKING_URI` | `http://localhost:${MLFLOW_PORT}` | client tracking endpoint |
| `MLFLOW_EXPERIMENT_NAME` | `bike-demand-training` | 모델 학습 experiment |
| `MLFLOW_MONITORING_EXPERIMENT_NAME` | `bike-demand-monitoring` | 월별 성능 점검 experiment |

호스트 프로세스는 `http://localhost:5000`, 같은 Compose network의 컨테이너는
`http://mlflow:5000`을 사용한다. AWS 학습 EC2는 app EC2의 private MLflow 주소를
환경에 주입한다.

공통 진입점 `libs/ml_core/mlflow_tracking.py`의 `configure()`는 tracking URI와
experiment만 설정한다. run 시작·종료와 실제 log 호출은 각 호출부 책임이다.

## 5. 학습 run

`training.train_common.train_target()` 호출 하나가 run 하나다. 기본 experiment는
`bike-demand-training`이며 run 이름은 다음과 같다.

```text
{model_name}_{profile_name}_d{TRAIN_DAY_DIVISOR}_h{MAX_TRAIN_HORIZON}
```

### 기록 params

- `model_name`, `profile_name`, `exposure_col`
- `train_window_start`, `train_window_end`
- `train_day_divisor`, `max_train_horizon`, `train_horizons`
- `adaptive_train_anchors`, 평일·휴일 peak 구간
- valid/test day-of-month와 split별 날짜 수
- feature table 경로, model archive prefix, feature 목록
- boosting round, early stopping, deferred valid, checkpoint 설정
- 실제 적용된 LightGBM params

삭제된 `train_year`와 `*_sample_frac`, 전체 DataFrame 기반 row params는 기록하지 않는다.
최종 row count는 metric으로 기록한다.

### 기록 metrics와 tags

- Poisson deviance, RMSE, best iteration
- Q10/Q50/Q90 pinball loss
- raw/calibrated P10–P90 coverage와 conformal correction
- train/valid/test row count
- 요청 boosting round, early-stopping 사용 여부
- wall time과 peak RSS
- phase별 progress percentage·round·validation metric
- `training_stage` tag

### MLflow artifact 사본

- Poisson, Q10, Q50, Q90 booster
- `station_categories.json`
- `conformal_correction.json`
- `profile.json`

`metrics.json`은 S3 model archive에 저장되지만 현재 MLflow에는 별도 JSON artifact가
아니라 numeric metrics로 기록된다. S3 archive가 원본이며 MLflow artifact는 UI에서
확인하기 위한 사본이다.

run은 context manager로 열리므로 학습 중 예외가 발생하면 MLflow 상태도 FAILED로
종료된다. 현재 training 경로에서 MLflow 설정·로그 실패는 자동으로 무시되지 않으므로
운영 학습 전 tracking server 상태를 확인한다.

## 6. 월별 모니터링 run

`monitor_performance.py`는 모델별 점검을 `bike-demand-monitoring` experiment에
기록한다.

- run 이름: `{model_name}_h{horizon}_{period_end}`
- params: model, horizon, 평가 시작·종료일
- metrics: row 수, baseline/current deviance·RMSE·coverage, 상대 변화, drift,
  `needs_retrain`
- artifact: 재학습 사유가 있을 때 `reasons.json`

모니터링의 MLflow 기록은 관측용이다. 서버가 없거나 로깅이 실패하면 경고만 출력하고
성능 판정과 재학습 필요 여부 계산은 계속한다.

## 7. 프로필 변경 이력

`libs/ml_core/profile_registry.py`의 `push_profile()`은 검증한 profile을 먼저
`profiles/{name}.json`에 저장하고 `bike-demand-profiles` experiment에도 params와
`profile.json`을 기록한다.

서비스가 읽는 authority는 S3 profile이다. MLflow의 profile run은 변경 감사 이력일
뿐 런타임 입력이 아니다. `builtin-default`는 코드 내장 profile이라 원격 registry에
등록할 수 없다.

## 8. UI에서 확인할 항목

`http://localhost:5000`에서 experiment를 선택하고 다음 컬럼을 우선 비교한다.

- `params.train_day_divisor`
- `params.train_horizons`
- `params.adaptive_train_anchors`
- `params.max_train_horizon`
- `metrics.poisson_deviance_test`
- `metrics.p10_p90_coverage_calibrated_test`
- `metrics.training_wall_time_seconds`
- `metrics.peak_rss_mb`

서로 다른 grid·anchor·horizon의 자체 test metric은 표본이 다를 수 있다. MLflow에서
수치가 나란히 보이더라도 공통 독립 test mart 없이 직접 우열을 결론내리지 않는다.

## 9. 연결 Smoke

실제 tracking server에 작은 run을 기록하려면 다음을 실행한다.

```bash
cd ml/training
set -a
source ../../.env
set +a

./.venv/bin/python -c "
from ml_core import mlflow_tracking
import mlflow

mlflow_tracking.configure('smoke-test')
with mlflow.start_run(run_name='smoke') as run:
    mlflow.log_params({'foo': 'bar'})
    mlflow.log_metrics({'rmse': 1.23})
    print(run.info.run_id)
"
```

UI에서 `smoke-test` run을 확인한 뒤 실제 실험 목록과 섞이지 않도록 삭제한다. 이
명령은 외부 tracking state를 변경하므로 단순 연결 확인이 필요할 때만 실행한다.

## 10. 테스트 격리

Training 테스트의 autouse fixture는 tracking URI를 `http://localhost:0`으로 바꿔
실제 서버에 run이 쌓이지 않게 한다. MLflow 자체를 검증하는 테스트는 임시 file
backend와 `MLFLOW_ALLOW_FILE_STORE=true`를 사용한다.

```bash
cd ml
./training/.venv/bin/python -m pytest \
  training/tests/dev_train_target_mlflow.py \
  training/tests/dev_monitor_performance.py -q
```

## 11. 장애 확인 순서

1. `mlflow` 컨테이너가 실행 중인지 확인한다.
2. MLflow 로그에서 PostgreSQL 연결 오류를 확인한다.
3. `mlflow` DB 존재 여부와 계정 권한을 확인한다.
4. MinIO bucket과 `mlflow-artifacts/` 쓰기 권한을 확인한다.
5. 클라이언트 위치에 맞는 `MLFLOW_TRACKING_URI`인지 확인한다.
6. AWS에서는 app EC2 보안그룹과 private port 5000 접근을 확인한다.

관련 구현은 `ops/compose/Dockerfile.mlflow`, Compose의 `mlflow` 서비스,
`libs/ml_core/mlflow_tracking.py` 및 `ml/training/train_common.py`다.
