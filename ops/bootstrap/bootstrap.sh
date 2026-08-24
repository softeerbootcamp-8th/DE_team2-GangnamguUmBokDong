#!/usr/bin/env bash
# 로컬 개발 인프라(postgres/minio/airflow)를 한 번에 띄우는 스크립트
set -euo pipefail

cd "$(dirname "$0")/../.."  # 저장소 루트로 이동

if [ ! -f .env ]; then
    echo "[bootstrap] .env가 없어 .env.example을 복사합니다."
    cp .env.example .env
fi

set -a
source .env
set +a

missing_keys=()
for key in SEOUL_OPENAPI_KEY KMA_APIHUB_KEY; do
    if [ -z "${!key:-}" ]; then
        missing_keys+=("${key}")
    fi
done
if (( ${#missing_keys[@]} > 0 )); then
    echo "[bootstrap] .env에 값이 필요한 API key: ${missing_keys[*]}" >&2
    echo "[bootstrap] 값을 채운 뒤 make bootstrap을 다시 실행하세요." >&2
    exit 2
fi

echo "[bootstrap] 운영 runtime을 로컬 Postgres / MinIO adapter로 기동합니다 (최초 실행 시 이미지 빌드로 시간이 걸릴 수 있습니다)..."
make up

echo "[bootstrap] 빈 저장소용 fixture를 게시하고 realtime_tick 전체 체인을 검증합니다..."
make e2e-smoke

cat <<EOF

[bootstrap] 완료. Gold baseline과 realtime_tick 전체 E2E가 성공했습니다.
[bootstrap] 운영 DAG는 pause 상태를 유지하므로 필요할 때 UI에서 활성화하세요.

  - Postgres       : localhost:${POSTGRES_PORT:-5432}  (db: ${POSTGRES_APP_DB:-app})
  - MinIO 콘솔     : http://localhost:${MINIO_CONSOLE_PORT:-9001}  (${MINIO_ROOT_USER:-minioadmin} / ${MINIO_ROOT_PASSWORD:-minioadmin})
  - Airflow 웹서버 : http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}  (user: ${AIRFLOW_ADMIN_USER:-admin})
  - Airflow 비밀번호: airflow/simple_auth_manager_passwords.json.generated
  - API            : http://localhost:${API_PORT:-8000}
  - Web (nginx)    : http://localhost:${WEB_PORT:-5173}  (${NGINX_BASIC_AUTH_USER:-admin} / ${NGINX_BASIC_AUTH_PASSWORD:-admin})

EOF
