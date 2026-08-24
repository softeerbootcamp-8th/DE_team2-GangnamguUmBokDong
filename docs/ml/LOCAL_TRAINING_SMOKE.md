# 실제 과거 자료 기반 로컬 학습 Smoke

> 현재 진입점: `bash ops/training_smoke.sh`
>
> 목적: 실제 ZIP을 Archive → Spark → LightGBM 경로로 관통하는 개발 검증

이 smoke는 `data/아카이브.zip`의 2025년 일부를 사용해 대여·반납 prototype
artifact를 만든다. 모델 품질을 판정하거나 champion·serving release를 승격하는
절차가 아니다.

## 1. 사전 조건

- Docker와 Docker Compose가 실행 가능해야 한다.
- `uv`가 설치돼 있어야 한다.
- 저장소 루트에 `data/아카이브.zip`이 있어야 한다.
- `.env` 또는 shell 환경에 `SEOUL_OPENAPI_KEY`가 있어야 한다.
- Spark 실행에 필요한 Java가 설치돼 있어야 한다.
- 기본 포트가 비어 있어야 한다.

| 서비스 | 기본 포트 |
|---|---:|
| MinIO API | 39000 |
| MinIO Console | 39001 |
| PostgreSQL | 35433 |
| MLflow | 35000 |

포트와 입력 경로는 다음 환경변수로 변경할 수 있다.

| 변수 | 용도 |
|---|---|
| `TRAINING_SMOKE_MINIO_PORT` | MinIO API 포트 |
| `TRAINING_SMOKE_MINIO_CONSOLE_PORT` | MinIO Console 포트 |
| `TRAINING_SMOKE_POSTGRES_PORT` | PostgreSQL 포트 |
| `TRAINING_SMOKE_MLFLOW_PORT` | MLflow 포트 |
| `TRAINING_SMOKE_ARCHIVE` | 원본 ZIP 경로 |
| `TRAINING_SMOKE_STAGE_ROOT` | staging·로그 디렉터리 |
| `TRAINING_SMOKE_ARCHIVE_DATE` | model archive 실행 ID |
| `LGB_NUM_THREADS` | LightGBM thread 수 |

## 2. 실행

저장소 루트에서 실행한다.

```bash
bash ops/training_smoke.sh
```

현재 Makefile에는 `training-smoke` target이 없으므로 `make training-smoke`는 사용하지
않는다.

스크립트는 격리된 Compose project `local-training-smoke`를 사용하며 기존 개발용
MinIO/PostgreSQL/MLflow와 다른 포트와 bucket을 사용한다.

## 3. 실제 실행 순서

```text
격리 MinIO·PostgreSQL·MLflow 시작
            │
            ▼
필요한 프로젝트 uv sync --frozen
            │
            ▼
ZIP에서 26일 원천만 staging
            │
            ▼
대여·재고·날씨·생활인구 Archive 적재
            │
            ▼
실제 재고+생활인구로 current station master 생성
            │
            ▼
champion pointer SHA snapshot
            │
            ▼
Spark base/multi-horizon feature 생성
            │
            ▼
rental·return prototype 학습
            │
            ▼
16개 artifact readback + pointer 불변 검증
```

가짜 정류소 ID는 만들지 않는다. `normalizer/local_training_master.py`가 실제
`bike_station_realtime`의 station ID·좌표와 생활인구 격자를 결합해
`station_master_enriched`를 만든다.

## 4. Smoke 전용 데이터 계약

| 항목 | 값 |
|---|---|
| ZIP staging·Archive 기간 | 2025-11-01~2025-11-26 |
| feature/training window | 2025-11-02~2025-11-19 |
| incremental lookback | 24시간 |
| model grid | 20분 |
| base training anchor | 60분 |
| adaptive anchor | 활성화 |
| peak anchor | 20분 |
| horizon | 1, 2 |
| LightGBM | 최대 20 rounds, early stopping 5 |
| Spark local master | `local[4]` |
| Spark driver memory | 6GiB |

`TRAIN_ANCHOR_TICK_MINUTES=60`이지만 adaptive anchor가 기본 활성화돼 있어 모든
학습 행이 60분 간격인 것은 아니다. 기본 peak 구간은 20분 anchor를 유지하고 평시는
60분, 심야는 3일에 한 번 60분 anchor를 사용한다.

운영 전체연도의 35일 lookback과 12개 생성 horizon 대신 smoke에서는 24시간과
2개 horizon으로 줄인다. 따라서 smoke 성공은 전체연도 메모리·시간이나 모델 품질을
보장하지 않는다.

## 5. 산출물과 검증

모델은 격리 bucket의 다음 archive에 저장된다.

```text
models/archive/dt=<실행별 고유 ID>/builtin-default/
```

`training.local_smoke_contract`는 다음 16개 artifact를 다시 읽는다.

- rental/return 각각 Poisson, Q10, Q50, Q90 booster
- rental/return 각각 station categories, conformal correction, metrics, profile

Booster는 LightGBM으로 다시 load하고 JSON을 parse한다. station category가 비었거나
metrics의 `model_name`이 다르면 실패한다. 실행 전후
`models/champion/*.json`의 key와 SHA-256도 비교하므로 학습이 champion pointer를
변경하면 실패한다.

이 검증은 `models/serving-release/current.json`을 게시하지 않는다. Smoke archive를
운영 serving release로 사용해서도 안 된다.

## 6. 로그와 확인 위치

기본 staging·로그 경로는 Git에서 제외되는 `data/local-training-smoke/`다.

| 경로 | 내용 |
|---|---|
| `logs/bootstrap-*.log` | source별 Archive 적재 로그 |
| `logs/bootstrap-living-population.log` | 생활인구 적재 로그 |
| `logs/feature-engine.log` | Spark feature 생성 로그 |
| `logs/rental-training.log` | 대여 학습 stdout |
| `logs/return-training.log` | 반납 학습 stdout |
| `logs/*-training-progress.log` | 날짜 chunk 진행과 peak RSS |

MLflow UI 기본 주소는 `http://localhost:35000`이다. 마지막 출력의 archive ID와
`SUCCESS`를 함께 확인한다.

## 7. 재실행과 정리

staging된 날씨 파일과 생활인구 26일이 모두 있으면 ZIP staging은 재사용한다.
그 외 Archive·모델 object는 같은 격리 bucket에 남는다. 매 실행의 기본 archive ID는
UTC timestamp를 포함해 서로 겹치지 않는다.

스크립트는 성공 후 Compose stack을 자동 종료하지 않는다. 확인이 끝나면 저장소
루트에서 명시적으로 내린다.

```bash
docker compose -p local-training-smoke \
  -f ops/compose/docker-compose.yml down
```

volume까지 제거하면 격리 MinIO·PostgreSQL 데이터가 복구되지 않으므로, 완전 초기화가
필요하다고 확인한 경우에만 별도로 삭제한다.

## 8. 성공 판정

다음 조건이 모두 만족돼야 smoke 성공이다.

1. 실제 원천의 일별 Archive 적재가 완료됐다.
2. Spark base feature와 multi-horizon mart가 생성됐다.
3. rental·return 네 objective가 모두 학습됐다.
4. 필수 artifact 16개를 다시 읽고 검증했다.
5. champion pointer가 실행 전후 동일하다.
6. 스크립트가 종료 코드 0과 `[training-smoke] SUCCESS`를 출력했다.
