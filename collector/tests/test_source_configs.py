"""Collector 소스 YAML 10종 테스트.

`sources/*.yaml` 전부가 config loader를 통과하는지, 이번에 늘어난 3개 키
(`max_missing_ratio` · `fetch` · `backfill`)가 생략 가능한지, 그리고 실제
어댑터(`seoul_openapi` · `kma_apihub`)를 공통 코드 수정 없이 pipeline까지
end-to-end로 통과시킬 수 있는지 확인한다.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import config.loader as config_loader
import httpx
import pipeline
import pytest
from adapters import (  # noqa: F401 — @adapter 등록을 위한 import
    kma_apihub,
    seoul_openapi,
)
from manifest import RunStatus

KST = ZoneInfo("Asia/Seoul")
SOURCES_DIR = Path(__file__).resolve().parent.parent / "sources"
SOURCE_IDS = sorted(p.stem for p in SOURCES_DIR.glob("*.yaml"))


@pytest.fixture(autouse=True)
def _api_keys(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "test-seoul-key")
    monkeypatch.setenv("KMA_APIHUB_KEY", "test-kma-key")


class TestAllSourcesLoad:
    def test_ten_sources_exist(self):
        assert len(SOURCE_IDS) == 10

    @pytest.mark.parametrize("source_id", SOURCE_IDS)
    def test_loads_without_error(self, source_id):
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)

        assert config.source_id == source_id
        assert config.adapter in ("seoul_openapi", "kma_apihub")
        assert config.config_version.startswith("sha256:")


class TestOptionalKeysOmittable:
    """새로 늘어난 3개 키가 생략 가능하고, 생략 시 기존과 같이 동작하는지 확인한다."""

    def test_fetch_omitted_falls_back_to_effective_budget(self):
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)

        assert config.fetch is None
        assert config.effective_fetch_budget() == config.schedule.interval / 2

    def test_backfill_omitted_defaults_to_disabled(self):
        config = config_loader.load("population_realtime", base_dir=SOURCES_DIR)

        assert config.backfill is None

    def test_max_missing_ratio_omitted_defaults_to_zero(self):
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)

        assert config.quality.max_missing_ratio == 0.05  # 명시한 값

    def test_max_missing_ratio_truly_omitted_defaults_to_zero(self, tmp_path):
        # 3개 키를 아예 안 쓴 최소 YAML로 기본값 자체를 확인한다.
        minimal = tmp_path / "minimal_source.yaml"
        minimal.write_text(
            """
source_id: minimal_source
description: 최소 설정 테스트용
adapter: seoul_openapi
adapter_params: {service: bikeList, page_size: 1000, root_key: rentBikeStatus.row}
schedule: {interval: 5m}
storage: {bronze_format: json, silver_format: parquet, partition: [dt, hh]}
quality: {max_drop_ratio: 0.05}
policies:
  required_missing: drop_row
  required_outlier: drop_row
  optional_missing: keep_null
  optional_outlier: set_null
columns: {}
"""
        )
        config = config_loader.load("minimal_source", base_dir=tmp_path)

        assert config.quality.max_missing_ratio == 0.0
        assert config.quality.allow_empty is False
        assert config.fetch is None
        assert config.backfill is None


def _seoul_response(wrapper_key: str, rows: list[dict], total: int | None = None) -> bytes:
    body = {wrapper_key: {"RESULT": {"CODE": "INFO-000"}, "row": rows}}
    if total is not None:
        body[wrapper_key]["list_total_count"] = total
    return json.dumps(body).encode()


class TestSeoulSourcesEndToEnd:
    """seoul_openapi 어댑터 하나로 소스 2개(bikeList·culturalEventInfo)가 그대로 도는지 확인한다.

    `cultural_event.yaml`은 공통 코드를 한 줄도 고치지 않고 YAML만 추가한 두 번째
    서울 열린데이터광장 소스다 — 이게 되면 "소스가 늘어도 공통 코드는 바뀌지
    않는다"는 설계 목표가 충족된 것으로 본다.
    """

    def test_bike_station_realtime_end_to_end(self, monkeypatch, tmp_path):
        config = config_loader.load("bike_station_realtime", base_dir=SOURCES_DIR)
        rows = [
            {
                "stationId": "ST-1", "stationName": "여의도역", "rackTotCnt": "15",
                "parkingBikeTotCnt": "7", "shared": "47", "stationLatitude": "37.52",
                "stationLongitude": "126.92",
            }
        ]

        def handler(request):
            return httpx.Response(200, content=_seoul_response("rentBikeStatus", rows, total=len(rows)))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, 14, 10, tzinfo=KST)

        result = pipeline.execute_window(config, window_start, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None

    def test_cultural_event_end_to_end_via_yaml_only(self, monkeypatch):
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)
        rows = [
            {
                "TITLE": "한강 밤도깨비 야시장", "CODENAME": "축제", "GUNAME": "영등포구",
                "PLACE": "여의도한강공원", "STRTDATE": "2026-08-01", "END_DATE": "2026-08-31",
                "IS_FREE": "무료", "LOT": "126.93", "LAT": "37.53",
            }
        ]

        def handler(request):
            return httpx.Response(200, content=_seoul_response("culturalEventInfo", rows, total=len(rows)))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, tzinfo=KST)

        result = pipeline.execute_window(config, window_start, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None

    def test_cultural_event_allows_zero_rows(self):
        config = config_loader.load("cultural_event", base_dir=SOURCES_DIR)

        def handler(request):
            return httpx.Response(200, content=_seoul_response("culturalEventInfo", [], total=0))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 13, tzinfo=KST)

        result = pipeline.execute_window(config, window_start, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.EMPTY

    def test_performance_event_end_to_end(self):
        config = config_loader.load("performance_event", base_dir=SOURCES_DIR)
        rows = [
            {
                "SCH_SEQ": "1234", "TITLE": "잠실 야구 경기", "SDATE": "2026-08-20",
                "EDATE": "2026-08-20", "USE_TIME": "18:30", "USE_AGE": "전체 관람가",
                "USE_TARGET": "시민", "USE_PAY": "유료", "LINK_URL": "https://example.com/event",
                "REG_DATE": "2026-08-01", "UPD_DATE": "2026-08-10", "SCH_CODE_A": "1",
                "SCH_CODE_B": "8", "CODE_TITLE_A": "스포츠경기", "CODE_TITLE_B": "잠실야구장",
            }
        ]

        def handler(request):
            return httpx.Response(200, content=_seoul_response("stadiumScheduleInfo", rows, total=len(rows)))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 18, tzinfo=KST)

        result = pipeline.execute_window(config, window_start, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.SUCCEEDED
        # allow_empty: true라서 status만 보면 스키마가 어긋나 전 행이 폐기돼도
        # 통과한다. 행이 실제로 살아남았는지까지 고정한다.
        assert result.counts.kept == 1
        assert result.drop_ratio == 0.0
        assert result.artifacts.silver is not None



class TestKmaSourceEndToEnd:
    def test_weather_ultra_short_live_end_to_end(self):
        config = config_loader.load("weather_ultra_short_live", base_dir=SOURCES_DIR)

        def handler(request):
            params = dict(request.url.params)
            common = {
                "nx": int(params["nx"]),
                "ny": int(params["ny"]),
                "baseDate": params["base_date"],
                "baseTime": params["base_time"],
            }
            items = [
                {**common, "category": "T1H", "obsrValue": "28.5"},
                {**common, "category": "REH", "obsrValue": "55"},
                {**common, "category": "WSD", "obsrValue": "2.1"},
                {**common, "category": "RN1", "obsrValue": "0"},
                {**common, "category": "PTY", "obsrValue": "0"},
            ]
            body = {"response": {"header": {"resultCode": "00"}, "body": {"items": {"item": items}}}}
            return httpx.Response(200, content=json.dumps(body).encode())

        client = httpx.Client(transport=httpx.MockTransport(handler))
        window_start = datetime(2026, 8, 12, 14, 0, tzinfo=KST)

        result = pipeline.execute_window(config, window_start, client=client, sleep_fn=lambda s: None)

        assert result.status == RunStatus.SUCCEEDED
        assert result.artifacts.silver is not None

    @pytest.mark.parametrize(
        "source_id", ["weather_ultra_short_live", "weather_short_term_forecast"]
    )
    def test_weather_grids_cover_25_seoul_gu_one_to_one(self, source_id):
        """loader/gu_mapping.py의 `_GRID_TO_GU_TABLE`은 여기 grids 목록과 1:1로
        맞춰 25개 구 전부를 대표하도록 만들어졌다. 격자를 늘리거나 줄일 때 loader
        쪽 테이블과 어긋나면 일부 구의 weather_current/weather_forecast가 조용히
        비게 되므로, 최소한 "정확히 25개, 중복 없음"은 여기서 회귀로 잡는다."""
        config = config_loader.load(source_id, base_dir=SOURCES_DIR)
        grids = config.adapter_params["grids"]

        assert len(grids) == 25
        assert len({tuple(g) for g in grids}) == 25
