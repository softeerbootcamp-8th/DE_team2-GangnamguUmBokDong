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
- Airflow 웹서버: `http://localhost:8080`

이후에는 아래 명령으로 다룹니다.

```bash
make up      # 기동
make down    # 종료
make logs    # 로그 확인
make ps      # 상태 확인
```

**중요**: `apps/api`, `apps/web`, `collector`, `ml/*`는 compose에 포함되어 있지 않습니다. 지금처럼 각자 `uv run`/`npm run`으로 로컬에서 직접 실행하고, 위 인프라(Postgres/MinIO)에만 연결해서 씁니다. Airflow만 예외적으로 컨테이너로 뜨는데, DAG 안에서 다른 프로젝트가 필요하면 저장소 전체가 마운트된 컨테이너 안에서 `cd collector && uv run python main.py`처럼 그대로 호출합니다 — 별도 이미지를 만들 필요는 없습니다.

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
