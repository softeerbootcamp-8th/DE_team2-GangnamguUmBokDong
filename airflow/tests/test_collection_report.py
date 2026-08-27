"""데이터 수집 모니터링 알림의 위험 판정·메시지 조합·정책 로딩·Slack 전송을 검증한다."""

from __future__ import annotations

import json

import notifications.collection_report as collection_report
import notifications.slack as slack
from config.alert_policy import load_thresholds
from notifications.collection_report import (
    build_daily_report_message,
    build_hourly_alert_message,
    evaluate_source_stats,
)


def _stats(**overrides) -> dict:
    base = {
        "run_count": 10,
        "status_counts": {"succeeded": 10},
        "missing_count": 0,
        "outlier_count": 0,
        "type_error_count": 0,
        "dropped_count": 0,
        "kept_count": 100,
        "max_drop_ratio": 0.0,
    }
    base.update(overrides)
    return base


def _evaluation(source_id: str, *, is_risky: bool) -> dict:
    return {
        "source_id": source_id,
        "stats": _stats(),
        "failure_rate": 0.0,
        "missing_ratio": 0.0,
        "outlier_ratio": 0.0,
        "is_risky": is_risky,
    }


def _fixed_thresholds(monkeypatch) -> None:
    monkeypatch.setattr(
        collection_report,
        "load_thresholds",
        lambda source_id: {
            "failure_rate_threshold": 0.2,
            "missing_ratio_threshold": 0.05,
            "outlier_ratio_threshold": 0.05,
        },
    )


class TestEvaluateSourceStats:
    def test_normal_stats_are_not_risky(self, monkeypatch):
        _fixed_thresholds(monkeypatch)

        result = evaluate_source_stats("bike_station_realtime", _stats())

        assert result["is_risky"] is False
        assert result["failure_rate"] == 0.0
        assert result["missing_ratio"] == 0.0
        assert result["outlier_ratio"] == 0.0

    def test_failure_rate_at_threshold_is_risky(self, monkeypatch):
        _fixed_thresholds(monkeypatch)
        stats = _stats(run_count=10, status_counts={"succeeded": 8, "failed": 2})

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["failure_rate"] == 0.2
        assert result["is_risky"] is True

    def test_missing_ratio_at_threshold_is_risky(self, monkeypatch):
        _fixed_thresholds(monkeypatch)
        stats = _stats(missing_count=5, kept_count=100)

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["missing_ratio"] == 0.05
        assert result["is_risky"] is True

    def test_outlier_ratio_at_threshold_is_risky(self, monkeypatch):
        _fixed_thresholds(monkeypatch)
        stats = _stats(outlier_count=5, kept_count=100)

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["outlier_ratio"] == 0.05
        assert result["is_risky"] is True

    def test_zero_kept_count_does_not_divide_by_zero(self, monkeypatch):
        _fixed_thresholds(monkeypatch)
        stats = _stats(kept_count=0, missing_count=0, outlier_count=0)

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["missing_ratio"] == 0.0
        assert result["outlier_ratio"] == 0.0
        assert result["is_risky"] is False

    def test_zero_run_count_does_not_divide_by_zero(self, monkeypatch):
        _fixed_thresholds(monkeypatch)
        stats = _stats(run_count=0, status_counts={})

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["failure_rate"] == 0.0

    def test_zero_run_count_is_risky(self, monkeypatch):
        """run_count == 0은 collector/Airflow가 그 기간 내내 완전히 멈춘, 비율
        임계값보다 심한 장애다 — 비율이 전부 0이어도 위험으로 판정해야 한다."""
        _fixed_thresholds(monkeypatch)
        stats = _stats(run_count=0, status_counts={})

        result = evaluate_source_stats("bike_station_realtime", stats)

        assert result["is_risky"] is True


class TestBuildDailyReportMessage:
    def test_lists_every_source_without_mentioning_group_when_none_risky(self, monkeypatch):
        monkeypatch.setattr(collection_report, "de2_group_mention", lambda: "<!subteam^TEST>")
        evaluations = [
            _evaluation("bike_station_realtime", is_risky=False),
            _evaluation("population_realtime", is_risky=False),
        ]

        message = build_daily_report_message("2026-08-26", evaluations)

        assert "bike_station_realtime" in message
        assert "population_realtime" in message
        assert "<!subteam^TEST>" not in message

    def test_tags_group_and_names_risky_sources_when_any_risky(self, monkeypatch):
        monkeypatch.setattr(collection_report, "de2_group_mention", lambda: "<!subteam^TEST>")
        evaluations = [
            _evaluation("bike_station_realtime", is_risky=False),
            _evaluation("weather_ultra_short_live", is_risky=True),
        ]

        message = build_daily_report_message("2026-08-26", evaluations)

        assert "<!subteam^TEST>" in message
        assert "weather_ultra_short_live" in message


class TestBuildHourlyAlertMessage:
    def test_returns_none_when_nothing_risky(self):
        assert build_hourly_alert_message("2026-08-26 07시", []) is None

    def test_mentions_group_and_lists_only_risky_sources(self, monkeypatch):
        monkeypatch.setattr(collection_report, "de2_group_mention", lambda: "<!subteam^TEST>")
        risky = [_evaluation("weather_ultra_short_live", is_risky=True)]

        message = build_hourly_alert_message("2026-08-26 07시", risky)

        assert message is not None
        assert "<!subteam^TEST>" in message
        assert "weather_ultra_short_live" in message


class TestLoadThresholds:
    def test_known_source_falls_back_to_default_when_no_override(self):
        assert load_thresholds("bike_station_realtime") == {
            "failure_rate_threshold": 0.2,
            "missing_ratio_threshold": 0.05,
            "outlier_ratio_threshold": 0.05,
        }

    def test_unknown_source_still_returns_default(self):
        thresholds = load_thresholds("not_a_real_source")

        assert thresholds["failure_rate_threshold"] == 0.2


class TestSlackSendMessage:
    def test_skips_without_raising_when_webhook_not_configured(self, monkeypatch):
        monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

        slack.send_message("hello")

    def test_posts_json_payload_when_webhook_configured(self, monkeypatch):
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
        captured = {}

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def fake_urlopen(request, timeout):
            captured["url"] = request.full_url
            captured["data"] = request.data
            captured["timeout"] = timeout
            return FakeResponse()

        monkeypatch.setattr(slack.urllib.request, "urlopen", fake_urlopen)

        slack.send_message("hello")

        assert captured["url"] == "https://hooks.slack.test/x"
        assert json.loads(captured["data"]) == {"text": "hello"}
        assert captured["timeout"] == 10


class TestDe2GroupMention:
    def test_falls_back_to_plain_text_without_group_id(self, monkeypatch):
        monkeypatch.delenv("SLACK_DE2_GROUP_ID", raising=False)

        assert slack.de2_group_mention() == "@de2조"

    def test_uses_subteam_mention_when_group_id_set(self, monkeypatch):
        monkeypatch.setenv("SLACK_DE2_GROUP_ID", "S0123ABC")

        assert slack.de2_group_mention() == "<!subteam^S0123ABC>"
