"""config.loader.load() 통합 테스트. tmp_path에 실제 YAML 파일을 써서 검증한다."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from config.loader import ConfigError, load

VALID_YAML = """\
source_id: test_source
description: 테스트 소스
adapter: seoul_openapi
adapter_params: {service: bikeList, page_size: 1000, root_key: rentBikeStatus.row}
schedule:
  interval: 5m
storage:
  bronze_format: json
  silver_format: parquet
  partition: [dt, hh]
quality:
  max_drop_ratio: 0.05
policies:
  required_missing: drop_row
  required_outlier: drop_row
  optional_missing: keep_null
  optional_outlier: set_null
columns:
  stationId:
    types: [str]
    required: true
"""


def _write(tmp_path: Path, source_id: str, content: str) -> Path:
    path = tmp_path / f"{source_id}.yaml"
    path.write_text(content)
    return tmp_path


class TestLoadHappyPath:
    def test_returns_source_config(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", VALID_YAML)
        config = load("test_source", base_dir=base_dir)
        assert config.source_id == "test_source"
        assert config.columns["stationId"].required is True

    def test_config_version_is_sha256(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", VALID_YAML)
        config = load("test_source", base_dir=base_dir)
        assert config.config_version.startswith("sha256:")
        assert len(config.config_version) == len("sha256:") + 64

    def test_hash_stable_across_reloads(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", VALID_YAML)
        first = load("test_source", base_dir=base_dir)
        second = load("test_source", base_dir=base_dir)
        assert first.config_version == second.config_version

    def test_hash_changes_with_content(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", VALID_YAML)
        first = load("test_source", base_dir=base_dir)
        changed = VALID_YAML.replace("테스트 소스", "다른 설명")
        _write(tmp_path, "test_source", changed)
        second = load("test_source", base_dir=base_dir)
        assert first.config_version != second.config_version


class TestLoadSourceIdMismatch:
    def test_filename_source_id_mismatch_raises(self, tmp_path):
        mismatched = VALID_YAML.replace("source_id: test_source", "source_id: other_source")
        base_dir = _write(tmp_path, "test_source", mismatched)
        with pytest.raises(ConfigError, match="test_source.*other_source|other_source.*test_source"):
            load("test_source", base_dir=base_dir)


class TestLoadStructuralFailure:
    def test_missing_required_field_raises(self, tmp_path):
        broken = VALID_YAML.replace("adapter: seoul_openapi\n", "")
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ValidationError):
            load("test_source", base_dir=base_dir)


class TestLoadPolicyNameValidation:
    def test_unknown_quadrant_policy_raises(self, tmp_path):
        broken = VALID_YAML.replace("required_missing: drop_row", "required_missing: drp_row")
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ConfigError, match="drp_row"):
            load("test_source", base_dir=base_dir)

    def test_unknown_column_override_raises(self, tmp_path):
        broken = VALID_YAML.replace(
            "    required: true\n", "    required: true\n    on_outlier: clip_to_rnge\n"
        )
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ConfigError, match="clip_to_rnge"):
            load("test_source", base_dir=base_dir)

    def test_unknown_row_policy_raises(self, tmp_path):
        broken = VALID_YAML.replace(
            "  optional_outlier: set_null\n",
            "  optional_outlier: set_null\n  row: drp_if_any_required_issue\n",
        )
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ConfigError, match="drp_if_any_required_issue"):
            load("test_source", base_dir=base_dir)

    def test_null_row_policy_skips_validation(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", VALID_YAML)
        config = load("test_source", base_dir=base_dir)
        assert config.policies.row is None

    def test_multiple_policy_errors_all_reported(self, tmp_path):
        broken = VALID_YAML.replace(
            "required_missing: drop_row", "required_missing: drp_row"
        ).replace("required_outlier: drop_row", "required_outlier: drp_row2")
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ConfigError) as exc_info:
            load("test_source", base_dir=base_dir)
        assert "drp_row" in str(exc_info.value)
        assert "drp_row2" in str(exc_info.value)

    def test_error_message_lists_registered_names(self, tmp_path):
        broken = VALID_YAML.replace("required_missing: drop_row", "required_missing: drp_row")
        base_dir = _write(tmp_path, "test_source", broken)
        with pytest.raises(ConfigError, match="keep_null"):
            load("test_source", base_dir=base_dir)


WITH_ROW_POLICY = VALID_YAML.replace(
    "  optional_outlier: set_null\n",
    "  optional_outlier: set_null\n  row: drop_if_issue_count_exceeds\n",
)


class TestLoadRowParamsValidation:
    def test_row_policy_missing_required_params_raises(self, tmp_path):
        base_dir = _write(tmp_path, "test_source", WITH_ROW_POLICY)
        with pytest.raises(ConfigError, match="row_params"):
            load("test_source", base_dir=base_dir)

    def test_row_policy_with_correct_params_passes(self, tmp_path):
        content = WITH_ROW_POLICY.replace(
            "  row: drop_if_issue_count_exceeds\n",
            "  row: drop_if_issue_count_exceeds\n  row_params: {max_issues: 3}\n",
        )
        base_dir = _write(tmp_path, "test_source", content)
        config = load("test_source", base_dir=base_dir)
        assert config.policies.row == "drop_if_issue_count_exceeds"
        assert config.policies.row_params == {"max_issues": 3}

    def test_row_policy_with_wrong_param_field_raises(self, tmp_path):
        content = WITH_ROW_POLICY.replace(
            "  row: drop_if_issue_count_exceeds\n",
            "  row: drop_if_issue_count_exceeds\n  row_params: {max_issue: 3}\n",
        )
        base_dir = _write(tmp_path, "test_source", content)
        with pytest.raises(ConfigError, match="row_params"):
            load("test_source", base_dir=base_dir)

    def test_row_policy_without_params_model_rejects_extra_row_params(self, tmp_path):
        content = VALID_YAML.replace(
            "  optional_outlier: set_null\n",
            "  optional_outlier: set_null\n  row: keep_always\n  row_params: {max_issues: 3}\n",
        )
        base_dir = _write(tmp_path, "test_source", content)
        with pytest.raises(ConfigError, match="row_params"):
            load("test_source", base_dir=base_dir)

    def test_row_policy_without_params_model_and_without_row_params_passes(self, tmp_path):
        content = VALID_YAML.replace(
            "  optional_outlier: set_null\n", "  optional_outlier: set_null\n  row: keep_always\n"
        )
        base_dir = _write(tmp_path, "test_source", content)
        config = load("test_source", base_dir=base_dir)
        assert config.policies.row == "keep_always"
        assert config.policies.row_params is None


class TestLoadAggregatesAllErrorKinds:
    def test_policy_name_and_row_params_errors_both_reported(self, tmp_path):
        content = VALID_YAML.replace(
            "required_missing: drop_row", "required_missing: drp_row"
        ).replace(
            "  optional_outlier: set_null\n",
            "  optional_outlier: set_null\n  row: drop_if_issue_count_exceeds\n",
        )
        base_dir = _write(tmp_path, "test_source", content)
        with pytest.raises(ConfigError) as exc_info:
            load("test_source", base_dir=base_dir)
        message = str(exc_info.value)
        assert "drp_row" in message
        assert "row_params" in message
