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
uv sync --frozen

case "${1:-api-server}" in
    init)
        uv run airflow db migrate
        # 5개 모듈은 Airflow와 별개의 uv 프로젝트다 — BashOperator가 처음 스케줄될 때
        # 콜드 네트워크 sync로 타임아웃을 먹지 않도록 컨테이너 기동 시 한 번 예열한다.
        # 다만 예열은 최적화일 뿐 Airflow DB 초기화의 성공 조건은 아니다. 일시적인
        # 패키지 다운로드 실패 하나가 전체 Compose 기동을 막지 않게 경고만 남긴다.
        # 실제 task는 각 모듈에서 `uv run --frozen`을 다시 실행하므로 lock 불일치나
        # 지속적인 다운로드 문제는 해당 task에서 명시적으로 실패한다.
        for proj in collector normalizer nowcaster ml/inference loader; do
            echo "[airflow-init] prewarming $proj"
            if ! (cd "/workspace/$proj" && uv sync --frozen); then
                echo "[airflow-init] WARNING: failed to prewarm $proj; task-time uv run will retry" >&2
            fi
        done
        ;;
    api-server)
        exec uv run airflow api-server
        ;;
    scheduler)
        exec uv run airflow scheduler
        ;;
    dag-processor)
        exec uv run airflow dag-processor
        ;;
    *)
        exec "$@"
        ;;
esac
