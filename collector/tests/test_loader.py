"""config.loader.load() 통합 테스트. tmp_path에 실제 YAML 파일을 써서 검증한다."""

from pathlib import Path

import pytest

from config.loader import ConfigError, load

VALID_YAML = """\
source_id: test_source
description: 테스트 소스
adapter: seoul_openapi
adapter_params: {}
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
        with pytest.raises(Exception):  # pydantic ValidationError
            load("test_source", base_dir=base_dir)
