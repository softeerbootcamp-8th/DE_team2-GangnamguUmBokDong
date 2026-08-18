#!/usr/bin/env bash
# airflow/ 프로젝트의 uv 환경을 항상 uv.lock 기준으로 맞춘 뒤,
# 인자에 따라 init(1회성 마이그레이션) / api-server / scheduler / dag-processor를 실행한다.
#
# Airflow 3.x부터 webserver 명령이 api-server로 개명되었고, DAG Processor가 스케줄러에
# 내장되지 않고 반드시 별도 프로세스로 떠야 한다. 기본 인증 매니저도 FAB에서 Simple
# Auth Manager로 바뀌어 admin 계정은 `airflow users create` 대신
# AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS 환경변수(docker-compose.yml)로 선언한다.
set -euo pipefail

cd /workspace/airflow
# CI의 `make sync-all`이 호스트 경로에 만든 airflow/.venv는 컨테이너 안에서
# interpreter 링크가 유효하지 않다. bind mount 위의 그 디렉터리를 uv가 지우고
# 다시 만드는 동안 `Directory not empty`가 발생할 수 있으므로, Airflow 자체 환경은
# 이미지/컨테이너 전용 경로에 둔다. 모듈별 BashOperator 환경과는 공유하지 않는다.
AIRFLOW_UV_ENVIRONMENT=/opt/venvs/airflow
UV_PROJECT_ENVIRONMENT="$AIRFLOW_UV_ENVIRONMENT" uv sync --frozen

case "${1:-api-server}" in
    init)
        UV_PROJECT_ENVIRONMENT="$AIRFLOW_UV_ENVIRONMENT" uv run airflow db migrate
        # 5개 모듈은 Airflow와 별개의 uv 프로젝트다 — BashOperator가 처음 스케줄될 때
        # 콜드 네트워크 sync로 타임아웃을 먹지 않도록 컨테이너 기동 시 한 번 예열한다.
        # 프로젝트명을 먼저 출력해 CI에서도 어느 lock 환경에서 실패했는지 알 수 있게 한다.
        for proj in collector normalizer nowcaster ml/inference loader; do
            echo "[airflow-init] prewarming $proj"
            (cd "/workspace/$proj" && uv sync --frozen)
        done
        ;;
    api-server)
        exec env UV_PROJECT_ENVIRONMENT="$AIRFLOW_UV_ENVIRONMENT" uv run airflow api-server
        ;;
    scheduler)
        exec env UV_PROJECT_ENVIRONMENT="$AIRFLOW_UV_ENVIRONMENT" uv run airflow scheduler
        ;;
    dag-processor)
        exec env UV_PROJECT_ENVIRONMENT="$AIRFLOW_UV_ENVIRONMENT" uv run airflow dag-processor
        ;;
    *)
        exec "$@"
        ;;
esac
