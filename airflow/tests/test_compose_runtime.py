"""Airflow Compose의 cross-platform 가상환경 격리 계약을 검증한다."""

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_PATH = _ROOT / "ops" / "compose" / "docker-compose.yml"
_PROD_COMPOSE_PATH = _ROOT / "ops" / "compose" / "docker-compose.prod.yml"
_ENV_EXAMPLE_PATH = _ROOT / ".env.example"
_ENTRYPOINT_PATH = _ROOT / "ops" / "compose" / "airflow-entrypoint.sh"
_AIRFLOW_SERVICES = (
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "airflow-dag-processor",
)
_MODULE_ENV_VOLUME = "airflow-module-venvs:/opt/venvs/modules"


def test_airflow_services_share_container_only_module_environments():
    """모든 Airflow 서비스가 host .venv 대신 같은 named volume을 사용한다."""
    compose = yaml.safe_load(_COMPOSE_PATH.read_text(encoding="utf-8"))

    assert "airflow-module-venvs" in compose["volumes"]
    for service_name in _AIRFLOW_SERVICES:
        assert _MODULE_ENV_VOLUME in compose["services"][service_name]["volumes"]


def test_airflow_init_prewarms_every_bash_operator_project():
    """초기화 단계가 추론과 재배치를 포함한 독립 uv 프로젝트를 모두 예열한다."""
    entrypoint = _ENTRYPOINT_PATH.read_text(encoding="utf-8")

    assert (
        "for proj in collector normalizer nowcaster ml/inference loader rebalance; do"
        in entrypoint
    )
    assert 'UV_PROJECT_ENVIRONMENT="/opt/venvs/modules/$env_name"' in entrypoint


def test_local_and_production_use_same_airflow_concurrency_contract():
    """개발·배포 환경이 같은 환경변수와 안전 기본 동시성을 사용한다."""
    local = yaml.safe_load(_COMPOSE_PATH.read_text(encoding="utf-8"))
    production = yaml.safe_load(_PROD_COMPOSE_PATH.read_text(encoding="utf-8"))
    expected = {
        "AIRFLOW__CORE__PARALLELISM": "${AIRFLOW__CORE__PARALLELISM:-3}",
        "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": (
            "${AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG:-2}"
        ),
    }

    for service_name in _AIRFLOW_SERVICES:
        environment = local["services"][service_name]["environment"]
        assert {key: environment[key] for key in expected} == expected

    production_environment = production["x-airflow-common"]["environment"]
    assert {
        key: production_environment[key] for key in expected
    } == expected

    env_example = _ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    assert "AIRFLOW__CORE__PARALLELISM=3" in env_example
    assert "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2" in env_example
