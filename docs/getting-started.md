# 로컬 개발 시작 가이드

> 기준 구현: 루트 `Makefile`, `ops/bootstrap/bootstrap.sh`,
> `ops/compose/docker-compose.yml`, `.env.example`

이 저장소는 하나의 uv workspace가 아니다. Python 컴포넌트는 각자의
`pyproject.toml`, `uv.lock`, `.venv`를 사용하고, 공통 로컬 서비스는 Docker
Compose로 실행한다.

## 1. 사전 요구사항

- Docker Desktop과 Docker Compose
- `make`
- Python 컴포넌트를 호스트에서 실행할 경우 [uv](https://docs.astral.sh/uv/)
- Web을 호스트에서 실행할 경우 Node.js 20 이상

```bash
docker compose version
uv --version
```

## 2. AWS 운영 runtime을 로컬에서 실행

로컬은 별도 개발 runtime이 아니라 AWS 운영 구성을 재현한다.
`docker-compose.prod.yml`의 Airflow/API/MLflow/nginx 정의를 그대로 사용하고,
AWS RDS와 S3만 Postgres와 MinIO로 치환한다. 저장소 루트에서 실행한다.

```bash
make bootstrap
```

`.env`가 없으면 `.env.example`을 복사한 뒤 API key가 없다는 안내와 함께 중단한다.
`SEOUL_OPENAPI_KEY`와 `KMA_APIHUB_KEY`를 채우고 다시 실행한다. 운영 이미지를 기동한
뒤 fixture 입력을 준비하고 `realtime_tick` 전체 DAG가 성공해야 bootstrap이 완료된다.

| 서비스 | 기본 주소 |
|---|---|
| Web(운영 nginx) | http://localhost:5173 |
| FastAPI | http://localhost:8000 |
| Airflow | http://localhost:8081 |
| MLflow | http://localhost:5000 |
| MinIO Console | http://localhost:9001 |
| PostgreSQL | `localhost:5433` |

Web의 기본 Basic Auth는 `admin / admin`이며 `.env`에서 변경할 수 있다. PostgreSQL
인스턴스 안에는 앱, Airflow, MLflow database가 분리돼 있다.

포트는 `.env`에서 바꿀 수 있다. Airflow 사용자명은 `AIRFLOW_ADMIN_USER`이며,
Airflow 3 Simple Auth Manager가 최초 생성한 비밀번호는 웹서버 로그에서 확인한다.

```bash
make ps
make logs
make down
make up
```

`make bootstrap`은 로컬 E2E fixture를 게시하지만 승인된 운영 Gold 기준 seed나 과거
DB migration을 자동 실행하지 않는다.

## 3. Gold 초기 데이터와 기존 볼륨

새 Gold DB에서 dispatch center와 weather grid 기준 정보가 필요할 때만 승인된 값으로
다음 명령을 실행한다.

```bash
GOLD_WEATHER_GRID_SEED_VERSION=local-dev-weather-grid-v1 \
GOLD_WEATHER_GRID_EFFECTIVE_DTTM=2026-08-19T03:15:38Z \
make bootstrap-gold-seeds
```

위 기본값은 현재 Makefile의 로컬 개발 기준이다. 운영 환경에서는 승인된 version과
effective time을 별도로 확정해야 한다. `make seed`는 의도적으로 비활성화돼 있다.

기존 Gold 볼륨에 route 취소·복원 schema가 없다면 스택을 실행한 상태에서 현재의
완결된 호환 migration을 적용한다.

```bash
make migrate-route-dismiss-restore
```

Compose는 기존 볼륨을 자동 변환하거나 삭제하지 않는다. schema check가 실패하면
볼륨을 바로 지우지 말고 로그, 백업 필요성, 적용할 migration을 먼저 확인한다.
영속 데이터와 재생성 가능한 cache는 다음처럼 구분된다.

| 종류 | Compose volume | 성격 |
|---|---|---|
| Gold·Airflow·MLflow DB | `umbokdong-dashboard-live_postgres-data` | 영속 데이터 |
| Bronze/Silver·모델 | `umbokdong-dashboard-live_minio-data` | 영속 데이터 |
| Airflow 모듈 환경 | `umbokdong-dashboard-live_airflow-module-venvs` | 재생성 가능 |
| Web 의존성 | `umbokdong-dashboard-live_web-node-modules` | 재생성 가능 |

PostgreSQL과 MinIO는 저장 형식과 복구 절차가 다르므로 하나의 물리 볼륨에 섞지
않는다. 전체 파이프라인을 다시 확인하려면 `make e2e-smoke`를 실행한다. 이 명령도
운영 Compose 원본의 Airflow에서 같은 DAG를 실행한다.

## 4. Python 프로젝트 설치

루트 Makefile이 관리하는 독립 uv 프로젝트는 12개다.

```text
collector            apps/api             airflow
normalizer           nowcaster            loader
rebalance            ml/feature_engine    ml/training
ml/inference         libs/core            libs/ml_core
```

전체 환경을 맞추려면 다음 명령을 사용한다.

```bash
make sync-all
```

한 컴포넌트만 작업한다면 그 디렉터리에서 독립적으로 실행할 수 있다.

```bash
cd collector
uv sync --frozen
uv run python main.py --help
```

의존성을 의도적으로 변경할 때만 `uv add` 또는 `uv remove`를 사용하고,
`pyproject.toml`과 `uv.lock`을 함께 반영한다. 평소 재현 설치에는 `--frozen`을 쓴다.

`libs/core`와 `libs/ml_core`는 실제 공유 라이브러리다. 다른 컴포넌트의 상대경로
editable dependency는 이미 각 `pyproject.toml`과 lockfile에 선언돼 있으므로 새로
`uv add`할 필요가 없다.

## 5. 테스트

```bash
make lint
make test
make test-ci
```

- `make test`는 Gold bootstrap 검증, 각 Python 프로젝트 테스트, Compose 안의
  Airflow 테스트를 실행한다.
- `make test-ci`는 CI 범위와 같은 프로젝트 묶음을 실행한다.
- 특정 컴포넌트는 해당 디렉터리에서 `uv run pytest -q`로 좁게 검증할 수 있다.
- Web은 `apps/web`에서 `npm ci`, `npm test`, `npm run build`로 검증한다.

실행 중인 로컬 스택의 실제 데이터 흐름은 다음으로 검사한다.

```bash
make e2e-smoke
```

E2E는 collector 호출과 Gold fixture를 포함하므로 `.env`, 실행 중인 서비스와 대상
logical time을 먼저 확인한다.

## 6. Apple Silicon

Makefile은 `Darwin/arm64`에서 PostGIS 서비스에
`docker-compose.apple-silicon.yml`을 자동 추가한다. 따라서 Apple Silicon에서도
`make bootstrap`, `make up`을 그대로 사용한다. Docker Desktop의 amd64 에뮬레이션을
사용할 수 있어야 한다.

Makefile 없이 직접 실행할 때만 두 Compose 파일을 함께 지정한다.

```bash
docker compose \
  --env-file ops/compose/local.defaults.env \
  --env-file .env \
  -f ops/compose/docker-compose.prod.yml \
  -f ops/compose/docker-compose.yml \
  -f ops/compose/docker-compose.apple-silicon.yml \
  up -d --build
```

이 override는 로컬 PostGIS용이며 운영 RDS에는 적용하지 않는다.

## 7. 자주 발생하는 오류

| 증상 | 확인할 것 |
|---|---|
| `postgres-schema-check` 실패 | `make logs`, 기존 PostGIS version과 migration 상태 |
| Airflow 로그인 실패 | `AIRFLOW_ADMIN_USER`, 웹서버 로그의 생성 비밀번호 |
| 수집 API 인증 실패 | `.env`의 서울시·기상청 API key |
| 호스트 Python에서 S3 접속 실패 | `S3_ENDPOINT_URL=http://localhost:9000`과 MinIO 자격증명 |
| 컨테이너에서 `localhost` 접속 실패 | Compose 내부 서비스명(`postgres`, `minio`, `mlflow`) 사용 여부 |
| 모델 또는 Gold 입력 부재 | `models/` seed, MinIO object, Gold seed publication 상태 |

운영 배포 명령은 로컬 시작 절차와 권한 범위가 다르므로 이 문서에서 다루지 않는다.
