"""Airflow Compose의 cross-platform 가상환경 격리 계약을 검증한다."""

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE_PATH = _ROOT / "ops" / "compose" / "docker-compose.yml"
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
