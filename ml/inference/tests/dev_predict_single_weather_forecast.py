"""_resolve_live_weather()가 target_ts(예측 시점)가 anchor_ts(T0, "지금")보다
미래일 때는 예보를, 그렇지 않을 때(또는 예보를 못 찾을 때)는 관측치를 쓰는지
검증한다.

2026-08: 예전엔 horizon과 무관하게 항상 `_get_recent_weather()`(관측, "지금 날씨"
재사용)만 썼다 — target_ts가 anchor_ts보다 3시간(`_get_recent_weather`의 기본
lookback_hours) 넘게 미래면 그 구간에 관측 데이터가 원천적으로 없어 ValueError로
죽었다. `_get_forecast_weather()`(`weather_short_term_forecast`) 연동으로 그 구간을
예보로 메운다 — collector의 예보 수집 브랜치가 아직 병합 전이라 실제 스키마는
가정이므로(`silver_schema.py` 참고), 여기서는 "예보를 찾으면 그걸 쓰고 못 찾으면
관측으로 폴백한다"는 계약만 검증한다(`_get_forecast_weather()` 내부 구현은 별도
테스트에서 검증).
"""

import json

import numpy as np
import pandas as pd
import pytest

from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _clear_weather_caches():
    """각 테스트가 독립된 inference run의 날씨 cache에서 시작하게 한다."""
    ps._clear_runtime_caches()
    yield
    ps._clear_runtime_caches()


def test_uses_observation_when_target_equals_anchor(monkeypatch):
    """horizon=1(target_ts==anchor_ts)이면 예보를 아예 조회하지 않고 관측만 쓴다."""
    anchor_ts = pd.Timestamp("2026-08-17 10:00:00")
    forecast_calls = []
    monkeypatch.setattr(
        ps, "_get_forecast_weather", lambda *a, **kw: forecast_calls.append(1) or None
    )
    monkeypatch.setattr(
        ps, "_get_recent_weather", lambda target_ts, **kw: {"temp": 20.0, "precip": 0.0}
    )

    temp, precip = ps._resolve_live_weather(anchor_ts, anchor_ts, None, None)

    assert not forecast_calls, "target_ts==anchor_ts인데 예보를 조회함"
    assert (temp, precip) == (20.0, 0.0)


def test_uses_observation_when_target_is_in_the_past(monkeypatch):
    """target_ts가 anchor_ts보다 과거(수집 지연 재현)면 예보를 조회하지 않는다."""
    anchor_ts = pd.Timestamp("2026-08-17 10:00:00")
    target_ts = anchor_ts - pd.Timedelta(minutes=10)
    forecast_calls = []
    monkeypatch.setattr(
        ps, "_get_forecast_weather", lambda *a, **kw: forecast_calls.append(1) or None
    )
    monkeypatch.setattr(
        ps, "_get_recent_weather", lambda target_ts, **kw: {"temp": 19.0, "precip": 0.5}
    )

    temp, precip = ps._resolve_live_weather(target_ts, anchor_ts, None, None)

    assert not forecast_calls
    assert (temp, precip) == (19.0, 0.5)


def test_uses_forecast_when_target_is_in_the_future(monkeypatch):
    """target_ts가 anchor_ts보다 미래(horizon>1)면 예보를 먼저 시도하고, 있으면 그걸 쓴다."""
    anchor_ts = pd.Timestamp("2026-08-17 10:00:00")
    target_ts = anchor_ts + pd.Timedelta(hours=5)
    observation_calls = []
    monkeypatch.setattr(
        ps,
        "_get_forecast_weather",
        lambda target_ts, **kw: {"temp": 25.0, "precip": 1.2},
    )
    monkeypatch.setattr(
        ps, "_get_recent_weather", lambda *a, **kw: observation_calls.append(1) or {}
    )

    temp, precip = ps._resolve_live_weather(target_ts, anchor_ts, None, None)

    assert not observation_calls, "예보를 찾았는데 관측치도 조회함"
    assert (temp, precip) == (25.0, 1.2)


def test_falls_back_to_observation_when_forecast_missing(monkeypatch):
    """미래 시각인데 예보를 못 찾으면(collector 미병합/수집 공백) 관측치(사실상 "지금
    날씨" 재사용)로 폴백한다 — 조용히 저하되지만 크래시하지 않는다."""
    anchor_ts = pd.Timestamp("2026-08-17 10:00:00")
    target_ts = anchor_ts + pd.Timedelta(hours=5)
    observation_timestamps = []
    monkeypatch.setattr(ps, "_get_forecast_weather", lambda target_ts, **kw: None)
    monkeypatch.setattr(
        ps,
        "_get_recent_weather",
        lambda observed_ts, **kw: (
            observation_timestamps.append(observed_ts) or {"temp": 21.0, "precip": 0.0}
        ),
    )

    temp, precip = ps._resolve_live_weather(target_ts, anchor_ts, None, None)

    assert (temp, precip) == (21.0, 0.0)
    assert observation_timestamps == [anchor_ts]


def test_explicit_values_skip_both_lookups(monkeypatch):
    """temp/precip을 직접 주면(테스트/디버깅용) 미래 시각이어도 조회 자체를 안 한다."""
    anchor_ts = pd.Timestamp("2026-08-17 10:00:00")
    target_ts = anchor_ts + pd.Timedelta(hours=5)
    calls = []
    monkeypatch.setattr(
        ps, "_get_forecast_weather", lambda *a, **kw: calls.append("forecast") or None
    )
    monkeypatch.setattr(
        ps, "_get_recent_weather", lambda *a, **kw: calls.append("obs") or {}
    )

    temp, precip = ps._resolve_live_weather(target_ts, anchor_ts, 30.0, 2.0)

    assert not calls
    assert (temp, precip) == (30.0, 2.0)


def _forecast_row(fcst_date: str, fcst_time: str, tmp: float, pcp) -> pd.DataFrame:
    # 실제 기상청 raw 스키마 그대로: 타겟 시각은 fcstDate(YYYYMMDD)+fcstTime(HHMM)
    # 두 컬럼으로 나뉘어 있고, PCP는 순수 숫자가 아니라 텍스트가 섞여 있다
    # (loader/transform.py의 weather_forecast_from_silver()가 이미 이 스키마로 읽음).
    return pd.DataFrame(
        [{"fcstDate": fcst_date, "fcstTime": fcst_time, "TMP": tmp, "PCP": pcp}]
    )


def test_get_forecast_weather_picks_nearest_row_from_latest_issue_file(monkeypatch):
    """가장 최근 발표 파일 안에서 target_ts와 가장 가까운 행을 고르고, PCP 텍스트를 mm로 파싱한다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    def _fake_read_many(keys, columns=None):
        # 최신 발표 파일(마지막 키)에 세 시각의 예보가 섞여 있고, target_ts(15:00)에
        # 가장 가까운 건 다른 행이 아니라 15:00 정각 행이어야 한다.
        latest = pd.concat(
            [
                _forecast_row("20260817", "1200", 20.0, "강수없음"),
                _forecast_row("20260817", "1500", 24.0, "1.5mm"),
                _forecast_row("20260817", "1800", 22.0, "1.0mm 미만"),
            ]
        )
        return [None] * (len(keys) - 1) + [latest]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    result = ps._get_forecast_weather(target_ts)

    assert result == {"temp": 24.0, "precip": 1.5}


def test_get_forecast_weather_averages_all_valid_grids_at_nearest_time(monkeypatch):
    """가장 가까운 예보 시각의 유효 격자를 전부 평균하고 깨진 격자는 제외한다."""
    target_ts = pd.Timestamp("2026-08-17 15:05:00")

    def _fake_read_many(keys, columns=None):
        latest = pd.concat(
            [
                _forecast_row("20260817", "1500", 20.0, "강수없음"),
                _forecast_row("20260817", "1500", 24.0, "2.0mm"),
                _forecast_row("20260817", "1500", 999.0, "1.0mm"),
                _forecast_row("20260817", "1800", 10.0, "100.0mm"),
            ]
        )
        return [None] * (len(keys) - 1) + [latest]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    result = ps._get_forecast_weather(target_ts)

    assert result == {"temp": 22.0, "precip": 1.0}


def test_get_forecast_weather_returns_none_when_no_files_found(monkeypatch):
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    monkeypatch.setattr(
        ps,
        "_read_authoritative_collector_snapshots",
        lambda keys, columns=None: [None] * len(keys),
    )

    assert ps._get_forecast_weather(target_ts) is None


def test_get_forecast_weather_returns_none_when_precip_unparseable(monkeypatch):
    """PCP가 파싱 불가능한 값이면(빈 문자열 등) None을 돌려줘 관측치 fallback으로 넘어가게 한다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    def _fake_read_many(keys, columns=None):
        return [None] * (len(keys) - 1) + [
            _forecast_row("20260817", "1500", 24.0, None)
        ]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_forecast_weather(target_ts) is None


def test_get_forecast_weather_coerces_broken_timestamp_and_uses_valid_row(monkeypatch):
    """깨진 예보 시각 한 행 때문에 파일 전체 조회가 실패하지 않아야 한다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    def _fake_read_many(keys, columns=None):
        latest = pd.concat(
            [
                _forecast_row("broken", "time", 99.0, "강수없음"),
                _forecast_row("20260817", "1500", 24.0, "1.5mm"),
            ]
        )
        return [None] * (len(keys) - 1) + [latest]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_forecast_weather(target_ts) == {"temp": 24.0, "precip": 1.5}


def test_get_forecast_weather_rejects_row_too_far_from_target(monkeypatch):
    """가까운 시각이 없는 발표본을 엉뚱한 target의 예보로 사용하지 않는다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    def _fake_read_many(keys, columns=None):
        distant = _forecast_row("20260817", "1800", 22.0, "강수없음")
        return [None] * (len(keys) - 1) + [distant]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_forecast_weather(target_ts) is None


def _observed_row(temp, precip) -> pd.DataFrame:
    """실황 Silver 원본 컬럼으로 관측 한 행을 만든다."""
    return pd.DataFrame([{"T1H": temp, "RN1": precip}])


def test_get_recent_weather_skips_invalid_latest_tick(monkeypatch):
    """최신 tick이 NaN이면 그대로 반환하지 않고 이전의 유효한 관측으로 돌아간다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        if len(calls) == 1:
            return [_observed_row(np.nan, "broken")]
        return [None] * (len(keys) - 1) + [_observed_row(23.0, 0.5)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_recent_weather(target_ts) == {"temp": 23.0, "precip": 0.5}
    assert [len(keys) for keys in calls] == [1, 36]


def test_get_recent_weather_skips_out_of_range_latest_tick(monkeypatch):
    """Collector 계약 범위를 벗어난 최신 관측도 이전 정상 tick으로 대체한다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")

    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        if len(calls) == 1:
            return [_observed_row(999.0, -1.0)]
        return [None] * (len(keys) - 1) + [_observed_row(23.0, 0.5)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_recent_weather(target_ts) == {"temp": 23.0, "precip": 0.5}
    assert [len(keys) for keys in calls] == [1, 36]


def _selection_metadata(stderr: str) -> list[dict]:
    """stderr의 source selection 구조화 로그만 JSON object로 파싱한다."""
    prefix = "Source selection metadata: "
    return [
        json.loads(line.removeprefix(prefix))
        for line in stderr.splitlines()
        if line.startswith(prefix)
    ]


def test_recent_weather_fresh_metadata_and_happy_path(monkeypatch, capsys):
    """Fresh 관측은 최신 한 건만 읽고 requested/selected를 같은 current로 남긴다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        return [_observed_row(23.0, 0.5)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_recent_weather(target_ts) == {"temp": 23.0, "precip": 0.5}

    metadata = _selection_metadata(capsys.readouterr().err)
    assert [len(keys) for keys in calls] == [1]
    assert len(metadata) == 1
    assert metadata[0]["status"] == "success"
    assert metadata[0]["freshness"] == "current"
    assert metadata[0]["requested_dttm"] == metadata[0]["selected_dttm"]


def test_recent_weather_stale_metadata_and_parallel_cold_miss(monkeypatch, capsys):
    """최신 miss는 나머지를 한 batch로 읽고 실제 fallback 시각을 stale로 남긴다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        if len(calls) == 1:
            return [None]
        return [None] * (len(keys) - 1) + [_observed_row(22.0, 0.0)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    assert ps._get_recent_weather(target_ts) == {"temp": 22.0, "precip": 0.0}

    metadata = _selection_metadata(capsys.readouterr().err)
    assert [len(keys) for keys in calls] == [1, 36]
    assert len(metadata) == 1
    assert metadata[0]["status"] == "success"
    assert metadata[0]["freshness"] == "stale"
    assert metadata[0]["selected_dttm"] < metadata[0]["requested_dttm"]


def test_recent_weather_full_miss_emits_one_failed_record(monkeypatch, capsys):
    """전체 결측은 hybrid 조회 뒤 selected 없이 failed provenance 한 건만 남긴다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        return [None] * len(keys)

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    with pytest.raises(ValueError, match="날씨 데이터가 없습니다"):
        ps._get_recent_weather(target_ts)

    metadata = _selection_metadata(capsys.readouterr().err)
    assert [len(keys) for keys in calls] == [1, 36]
    assert len(metadata) == 1
    assert metadata[0]["status"] == "failed"
    assert metadata[0]["resolution"] == "unavailable"
    assert metadata[0]["selected_dttm"] is None


def test_recent_weather_cache_hit_preserves_provenance(monkeypatch, capsys):
    """같은 query의 result cache hit도 I/O 없이 selection metadata를 다시 남긴다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys)
        return [_observed_row(23.0, 0.5)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    first = ps._get_recent_weather(target_ts)
    second = ps._get_recent_weather(target_ts)

    metadata = _selection_metadata(capsys.readouterr().err)
    assert first == second == {"temp": 23.0, "precip": 0.5}
    assert [len(keys) for keys in calls] == [1]
    assert len(metadata) == 2
    assert metadata[0] == metadata[1]


def test_recent_weather_reuses_same_anchor_result(monkeypatch):
    """예보 실패가 horizon마다 반복돼도 같은 관측 fallback은 한 번만 읽는다."""
    target_ts = pd.Timestamp("2026-08-17 15:00:00")
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys[0])
        return [_observed_row(23.0, 0.5)]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    first = ps._get_recent_weather(target_ts)
    second = ps._get_recent_weather(target_ts)

    assert first == second == {"temp": 23.0, "precip": 0.5}
    assert len(calls) == 1


def test_forecast_reuses_same_issue_snapshot_across_targets(monkeypatch):
    """같은 발표본에 든 서로 다른 horizon은 source snapshot을 다시 읽지 않는다."""
    snapshots = pd.concat(
        [
            _forecast_row("20260817", "1500", 24.0, "1.5mm"),
            _forecast_row("20260817", "1600", 25.0, "강수없음"),
        ]
    )
    calls = []

    def _fake_read_many(keys, columns=None):
        calls.append(keys[0])
        return [snapshots]

    monkeypatch.setattr(ps, "_read_authoritative_collector_snapshots", _fake_read_many)

    at_15 = ps._get_forecast_weather(pd.Timestamp("2026-08-17 15:00:00"))
    at_16 = ps._get_forecast_weather(pd.Timestamp("2026-08-17 16:00:00"))

    assert at_15 == {"temp": 24.0, "precip": 1.5}
    assert at_16 == {"temp": 25.0, "precip": 0.0}
    assert len(calls) == 1
