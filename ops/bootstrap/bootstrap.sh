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

echo "[bootstrap] postgres / minio / airflow를 기동합니다 (최초 실행 시 이미지 빌드로 시간이 걸릴 수 있습니다)..."
make up

cat <<EOF

[bootstrap] 완료. #129 Gold PostGIS baseline 확인을 통과했습니다.
[bootstrap] collector와 ml/*는 각 프로젝트에서 uv run으로 로컬 실행하면 됩니다.

  - Postgres       : localhost:${POSTGRES_PORT:-5432}  (db: ${POSTGRES_APP_DB:-app})
  - MinIO 콘솔     : http://localhost:${MINIO_CONSOLE_PORT:-9001}  (${MINIO_ROOT_USER:-minioadmin} / ${MINIO_ROOT_PASSWORD:-minioadmin})
  - Airflow 웹서버 : http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}  (${AIRFLOW_ADMIN_USER:-admin} / ${AIRFLOW_ADMIN_PASSWORD:-admin})
  - API            : http://localhost:${API_PORT:-8000}
  - Web            : http://localhost:${WEB_PORT:-5173}

EOF
