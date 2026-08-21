# 개발 환경 가이드 

이 프로젝트는 [uv](https://docs.astral.sh/uv/) **워크스페이스를 쓰지 않습니다.** 아래 7개 폴더가 각각 완전히 독립된 uv 프로젝트입니다 — 각자 자기 `pyproject.toml`과 `uv.lock`을 가지고, 자기만의 `.venv`를 만듭니다. 한 프로젝트의 의존성을 바꿔도 다른 프로젝트에는 전혀 영향이 없습니다.

- `airflow/` (패키지명 `airflow-dags`)
- `collector/`
- `apps/api/` (패키지명 `api`)
- `ml/predict/`
- `ml/training/`
- `ml/feature/`
- `libs/core/` (패키지명 `core`) — 위 6개가 공용으로 가져다 쓰는 공유 라이브러리

`apps/web`은 React(Node.js) 프로젝트라 uv/Python과 무관합니다. npm/pnpm으로 별도 관리합니다.

## 로컬 인프라 띄우기 (Postgres / MinIO / Airflow)

`airflow`, `apps`, `ml`, `collector`가 공통으로 의존하는 인프라(Postgres, MinIO, Airflow)는 `ops/compose`의 docker compose로 관리합니다. 최초 1회 아래 명령이면 충분합니다.

```bash
make bootstrap
```

`.env`가 없으면 `.env.example`에서 자동으로 복사하고, Postgres / MinIO / Airflow(webserver+scheduler)를 기동합니다. 완료되면 아래 주소로 접속할 수 있습니다.

- Postgres: `localhost:5432` (앱 DB: `app`, Airflow 메타데이터 DB: `airflow` — 인스턴스 하나에 분리 생성됨)
- MinIO 콘솔: `http://localhost:9001`
- Airflow 웹서버: `http://localhost:8081`

이후에는 아래 명령으로 다룹니다.

```bash
make up      # 기동
make down    # 종료
make logs    # 로그 확인
make ps      # 상태 확인
```

### Apple Silicon에서 PostGIS 실행

로컬 Compose가 사용하는 `postgis/postgis:16-3.4` 이미지는 `linux/amd64`만
배포됩니다. M1/M2/M3/M4 Mac의 Docker Linux VM은 기본 `linux/arm64`이므로 플랫폼을
명시하지 않으면 `no matching manifest for linux/arm64/v8` 오류가 발생합니다.

Makefile은 `Darwin/arm64` 호스트에서만
`docker-compose.apple-silicon.yml`을 자동으로 추가합니다. 이 override는 `postgres`와
`postgres-schema-check` 두 서비스에만 `linux/amd64`를 적용합니다. 따라서 Apple
Silicon에서도 기존과 동일하게 `make bootstrap` 또는 `make up`을 실행하면 됩니다.
Docker Desktop의 x86_64/amd64 에뮬레이션(Rosetta) 옵션은 활성화하는 것을
권장합니다.

이 설정은 로컬 개발용 PostGIS 컨테이너에만 적용됩니다. 운영 AWS의 RDS에는 해당
이미지를 사용하지 않으며, MinIO·Airflow·Node 이미지는 각 호스트 아키텍처의 native
이미지를 계속 사용합니다. Linux ARM/Graviton에는 Apple 전용 override를 적용하지
않습니다. Graviton EC2에서 로컬 Compose 전체와 PostGIS까지 실행하는 구성은 현재
지원하지 않으므로, RDS를 사용하거나 검증된 multi-arch PostGIS 이미지가 필요합니다.
x86_64 EC2에서는 기존 Compose를 그대로 실행할 수 있습니다.

Makefile을 통하지 않고 Docker Compose를 직접 실행해야 하는 Apple Silicon 환경에서는
두 파일을 함께 지정합니다.

```bash
docker compose \
  -f ops/compose/docker-compose.yml \
  -f ops/compose/docker-compose.apple-silicon.yml \
  up -d --build
```

### Gold PostGIS baseline과 기존 볼륨

로컬 PostgreSQL은 `postgis/postgis:16-3.4`를 사용하며, **새 `postgres-data` 볼륨을
처음 초기화할 때만** [Gold 스키마 SSOT](gold/target-schema.sql)를 적용합니다. 이후
기동에서는 스키마 DDL을 다시 실행하지 않습니다.

> **PostGIS 3.5 → 3.4 변경 (2026-08-21)**
>
> 운영 RDS(PostgreSQL 16.14)가 **PostGIS 3.4.6만 제공**해서, dev/prod를 같은 조합으로
> 맞추기 위해 로컬 이미지도 3.4로 내렸습니다. 스키마·함수 18개·트리거 35개·GiST 3개·ACL이
> 3.4에서 전부 통과함을 확인했습니다(`check_gold_schema.sql`의 버전 조건도 3.4로 변경).
>
> **기존 볼륨을 쓰던 사람은 반드시 볼륨을 새로 만들어야 합니다.** 3.5로 초기화된 볼륨에
> 3.4 이미지를 붙이면 `check_gold_schema.sh`가 버전 불일치로 exit 78을 냅니다.
>
> ```bash
> make down
> docker volume rm de-team2-gangnamguumbokdong_postgres-data
> make up
> ```

과거 `postgres:16` 스키마가 든 볼륨을 발견하면 PostgreSQL을 시작하기 전에 명확한
오류로 중단합니다. Compose는 기존 볼륨을 변환하거나 삭제하지 않습니다. 기존 볼륨을
보존하면서 새 Gold 개발 환경을 만들려면 먼저 컨테이너만 내리고(`make down`은 named
volume을 삭제하지 않습니다), 별도 Compose 프로젝트 이름으로 기동합니다.

```bash
make down
COMPOSE_PROJECT_NAME=gold-postgis-v1 make up
```

이후 `logs`, `ps`, `down`에도 같은 `COMPOSE_PROJECT_NAME`을 붙여야 같은 환경을
다룹니다. 구 스키마에 직접 쓰던 `make seed`와 `apps/api/seed_gold.py`는 비활성화되어
있습니다. 로컬 fixture는 후속 #152의 source publisher 경로가 준비된 뒤 그 경로로
적재해야 합니다.

**중요**: Compose는 로컬 확인용 `apps/api`와 `apps/web`도 함께 기동합니다.
`collector`와 `ml/*`는 Compose에 포함되지 않으므로 각 프로젝트에서 `uv run`으로
직접 실행합니다. Airflow DAG에서 다른 프로젝트가 필요하면 저장소 전체가 마운트된
컨테이너 안에서 `cd collector && uv run python main.py`처럼 호출합니다.

## 사전 준비

uv가 설치되어 있는지 확인합니다.

```bash
uv --version
```

없다면 설치합니다. (macOS / Linux)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 처음 받았을 때 (최초 1회)

각 프로젝트 폴더로 들어가서 그 프로젝트만 `uv sync`합니다. **워크스페이스가 아니므로 루트에서 한 번에 `uv sync`하는 명령은 없습니다.**

```bash
git clone [저장소 주소]
cd my-workspace

cd collector && uv sync && cd ..
cd apps/api && uv sync && cd ../..
cd airflow && uv sync && cd ..
cd ml/predict && uv sync && cd ../..
cd ml/training && uv sync && cd ../..
cd ml/feature && uv sync && cd ../..
cd libs/core && uv sync && cd ../..
```

작업할 프로젝트만 골라서 `uv sync`해도 됩니다. 예를 들어 `collector` 작업만 한다면 `cd collector && uv sync`만 실행하면 충분합니다.

## 매일 하는 작업

### 최신 코드 받은 뒤 의존성 맞추기

작업 중인 프로젝트 폴더에서:

```bash
git pull
cd <프로젝트 폴더>   # 예: cd collector
uv sync
```

### 코드 실행

프로젝트 폴더 안으로 들어가서 실행합니다. (워크스페이스가 아니므로 `--package` 플래그는 없습니다.)

```bash
cd collector
uv run python main.py
```

## 의존성 추가 · 삭제

작업 중인 프로젝트 폴더 안에서 실행합니다.

### 라이브러리 추가

```bash
cd collector
uv add requests
```

### 라이브러리 삭제

```bash
cd collector
uv remove requests
```

### libs/core(공유 라이브러리) 의존 걸기

`libs/core`를 경로 기반 + 수정 가능(editable) 의존성으로 연결합니다. 이미 6개 프로젝트 모두 연결되어 있지만, 새 프로젝트를 추가하는 경우 아래처럼 직접 걸 수 있습니다.

```bash
cd collector
uv add ../libs/core --editable
```

(경로 깊이는 프로젝트 위치에 따라 다릅니다. `apps/api`, `ml/predict` 등 2단계 깊이는 `../../libs/core`.)

`editable`로 연결하면 `libs/core`의 코드를 수정했을 때 재설치 없이 바로 반영됩니다.

> 의존성을 바꾸면 `pyproject.toml`과 그 프로젝트의 `uv.lock`이 함께 갱신됩니다.
> **두 파일 모두 git에 커밋**해야 합니다. 다른 프로젝트의 `uv.lock`은 영향받지 않습니다.

## 꼭 기억할 규칙

- 모든 uv 명령은 **해당 프로젝트 폴더 안에서** 실행합니다 (`cd <프로젝트> && uv ...`). 루트에서 실행하는 워크스페이스 명령은 없습니다.
- `pyproject.toml`과 `uv.lock`은 프로젝트별로 **각각 커밋**하고, `.venv`는 커밋하지 않습니다.

## 프로젝트 구성 시 주의사항

- **레이아웃은 두 종류로 나뉩니다.**
  - `libs/core`는 다른 프로젝트가 `import core`로 가져다 쓰는 **실제 라이브러리**라서, 표준 src 레이아웃(`libs/core/src/core/__init__.py`)을 쓰고 `hatchling`으로 wheel을 빌드합니다.
  - 나머지 6개(`airflow`, `collector`, `apps/api`, `ml/predict`, `ml/training`, `ml/feature`)는 아무도 `import`해서 쓰지 않는 **실행형 애플리케이션**입니다. 패키지 폴더 없이 `pyproject.toml`과 같은 위치에 `.py` 파일을 바로 두는 완전 평탄화 구조를 쓰고, `[tool.uv] package = false`로 wheel 빌드를 하지 않습니다. `uv run python <파일>`로 직접 실행합니다.
- `ml/predict`(추론), `ml/training`(모델 학습), `ml/feature`(피처마트 생성)는 서로 독립적인 프로젝트이며 배포도 각각 따로 합니다. 세 프로젝트는 서로 import하지 않습니다.


## 명령어 요약

| 상황 | 명령어 |
| --- | --- |
| 최초 환경 구성 | `cd <프로젝트> && uv sync` |
| 최신 코드 반영 | `git pull && cd <프로젝트> && uv sync` |
| 코드 실행 | `cd <프로젝트> && uv run python <파일>` |
| 라이브러리 추가 | `cd <프로젝트> && uv add <라이브러리>` |
| 라이브러리 삭제 | `cd <프로젝트> && uv remove <라이브러리>` |
| libs/core 연결 | `cd <프로젝트> && uv add <libs/core까지의 상대경로> --editable` |
