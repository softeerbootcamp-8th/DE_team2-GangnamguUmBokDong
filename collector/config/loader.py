"""sources/{source_id}.yaml을 읽어 검증까지 마친 SourceConfig로 만드는 곳.

파일을 읽고, Pydantic으로 타입을 검사하고, 정책 이름이 실제 등록된 것인지 확인한 뒤 문제가 없으면 SourceConfig를 반환한다. 
여기서 오류를 다 걸러내야 네트워크를 타기 전에 잘못된 설정을 잡을 수 있다. 
통과한 설정에는 원본 YAML의 SHA-256 해시를 `config_version`으로 붙여, 나중에 어떤 설정으로 수집했는지 추적할 수 있게 한다.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import yaml
from pydantic import ValidationError

from config.schema import SourceConfig
from validation.registry import (
    get_row_policy_params_model,
    is_policy_registered,
    is_row_policy_registered,
    policy_names,
    row_policy_names,
)


class ConfigError(ValueError):
    """논리적 설정 오류를 알려주는 Error"""


def load(source_id: str, base_dir: Path = Path("sources")) -> SourceConfig:
    """소스 ID를 받아 YAML을 읽고 Pydantic으로 검증하여 최종 소스 설정 객체를 반환한다."""

    raw_bytes = (base_dir / f"{source_id}.yaml").read_bytes()

    # 읽어온 원문 바이트에 대해서 SHA-256 해시를 떠서 config_version으로 기록한다.
    # 나중에 정확히 어떤 설정 상태일 때 수집된 데이터인지 추적하기 위함
    config_version = f"sha256:{hashlib.sha256(raw_bytes).hexdigest()}"

    raw = yaml.safe_load(raw_bytes)

    # 만약 여기서 필수값이 빠졌거나 타입이 틀렸으면 ValidationError가 발생한다.
    config = SourceConfig.model_validate(raw)

    errors: list[str] = []
    if config.source_id != source_id:
        errors.append(
            f"파일명 '{source_id}'와 YAML 안의 source_id '{config.source_id}'가 다릅니다."
        )
    errors += _check_policy_names(config)
    errors += _check_row_params(config)
    if errors:
        raise ConfigError("\n".join(errors))

    return config.model_copy(update={"config_version": config_version})


def _check_policy_names(config: SourceConfig) -> list[str]:
    """기본 컬럼 정책, 개별 컬럼 오버라이드 정책, 행 정책 검사"""
    errors: list[str] = []

    # 기본 컬럼 정책
    quadrants = {
        "policies.required_missing": config.policies.required_missing,
        "policies.required_outlier": config.policies.required_outlier,
        "policies.optional_missing": config.policies.optional_missing,
        "policies.optional_outlier": config.policies.optional_outlier,
    }
    for location, name in quadrants.items():
        if not is_policy_registered(name):
            errors.append(_unregistered_message(location, name, policy_names()))

    for column_name, spec in config.columns.items():
        for field in ("on_missing", "on_outlier"):
            name = getattr(spec, field)
            if name is not None and not is_policy_registered(name):
                location = f"columns.{column_name}.{field}"
                errors.append(_unregistered_message(location, name, policy_names()))

    if config.policies.row is not None and not is_row_policy_registered(config.policies.row):
        errors.append(
            _unregistered_message("policies.row", config.policies.row, row_policy_names())
        )

    return errors


def _unregistered_message(location: str, name: str, registered: tuple[str, ...]) -> str:
    """미등록 정책 이름 오류 메시지를 만든다."""
    listed = ", ".join(registered) or "(없음)"
    return f"{location}: '{name}'이 등록돼 있지 않습니다. 등록된 이름: {listed}"


def _check_row_params(config: SourceConfig) -> list[str]:
    """행 정책을 사용할 때 넘겨주는 추가 설정값들이 정상적인지 검증하는 역할"""

    row_name = config.policies.row
    if row_name is None or not is_row_policy_registered(row_name):
        return []  # 미등록 이름은 _check_policy_names가 이미 보고한다.

    params_model = get_row_policy_params_model(row_name)
    row_params = config.policies.row_params

    if params_model is None:
        if row_params is not None:
            return [
                f"policies.row_params: '{row_name}'은 params를 받지 않는데, row_params가 주어졌습니다: {row_params}"
            ]
        return []

    try:
        params_model.model_validate(row_params or {})
    except ValidationError as exc:
        return [f"policies.row_params: '{row_name}'의 params 형식이 아닙니다.: {exc}"]
    return []
