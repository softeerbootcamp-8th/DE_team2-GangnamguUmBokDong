"""3개 weather yaml의 grids 목록이 항상 동일한지 검사해 드리프트를 막는다."""

from pathlib import Path

import yaml

_SOURCES_DIR = Path(__file__).resolve().parents[1] / "sources"
_WEATHER_SOURCE_FILES = [
    "weather_ultra_short_live.yaml",
    "weather_ultra_short_forecast.yaml",
    "weather_short_term_forecast.yaml",
]


def _grids(filename: str) -> list[list[int]]:
    raw = yaml.safe_load((_SOURCES_DIR / filename).read_text(encoding="utf-8"))
    return raw["adapter_params"]["grids"]


def test_weather_yaml_grids_are_identical_across_sources():
    grids_by_file = {name: _grids(name) for name in _WEATHER_SOURCE_FILES}
    reference = grids_by_file[_WEATHER_SOURCE_FILES[0]]
    for name, grids in grids_by_file.items():
        assert grids == reference, f"{name}의 grids가 다른 소스와 다르다"


def test_weather_yaml_grids_have_34_unique_entries():
    grids = _grids(_WEATHER_SOURCE_FILES[0])
    assert len(grids) == len(set(map(tuple, grids)))
    assert len(grids) == 34
