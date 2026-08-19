"""서울 실제 대여소 좌표를 기준으로 날씨 수집 격자 목록을 생성한다.

collector/sources/weather_*.yaml 3개 파일의 adapter_params.grids를 동일한 값으로
갱신해, 파일마다 손으로 복사해 둔 격자 목록이 서로 어긋나는 것을 막는다.

실행 시점: 이번에 1회. 이후로는 서울 자전거망이 확장돼 기존 격자 밖에 새 대여소가
생기면 재실행한다(자주 바뀌는 게 아니라서 자동화하지 않는다).
"""

from __future__ import annotations

import json
from pathlib import Path

from gu_mapping import latlon_to_grid

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STATIONS_PATH = _REPO_ROOT / "apps" / "api" / "seed_data" / "stations_seoul.json"
_WEATHER_YAML_PATHS = [
    _REPO_ROOT / "collector" / "sources" / "weather_ultra_short_live.yaml",
    _REPO_ROOT / "collector" / "sources" / "weather_ultra_short_forecast.yaml",
    _REPO_ROOT / "collector" / "sources" / "weather_short_term_forecast.yaml",
]


def compute_grids() -> list[tuple[int, int]]:
    """실제 대여소 좌표 전부를 격자로 변환해 정렬된 고유 격자 목록을 반환한다."""
    stations = json.loads(_STATIONS_PATH.read_text(encoding="utf-8"))
    grid_set = {latlon_to_grid(s["lat"], s["lon"]) for s in stations}
    return sorted(grid_set)


def _update_grids_block(path: Path, grids: list[tuple[int, int]]) -> None:
    """yaml 파일의 adapter_params.grids 블록만 정확히 교체한다(다른 설정은 그대로 둔다)."""
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    start = next(i for i, line in enumerate(lines) if line.strip() == "grids:")
    end = start + 1
    while end < len(lines) and lines[end].lstrip().startswith("- "):
        end += 1
    new_block = [f"  - [{nx}, {ny}]\n" for nx, ny in grids]
    path.write_text("".join(lines[: start + 1] + new_block + lines[end:]), encoding="utf-8")


def main() -> None:
    grids = compute_grids()
    print(f"{len(grids)}개 격자 계산됨")
    for path in _WEATHER_YAML_PATHS:
        _update_grids_block(path, grids)
        print(f"갱신: {path}")


if __name__ == "__main__":
    main()
