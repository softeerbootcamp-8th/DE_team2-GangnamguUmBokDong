"""main.py 테스트: 인자 파싱, --force/--backfill 동시 지정 차단, 종료 코드 매핑."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import main
from manifest import RunStatus

KST = ZoneInfo("Asia/Seoul")


class TestParseArgs:
    def test_parses_required_and_optional_flags(self):
        args = main.parse_args(
            ["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00", "--force"]
        )

        assert args.source == "bike_station_realtime"
        assert args.window_start == "2026-08-12T14:10:00+09:00"
        assert args.force is True
        assert args.backfill is False

    def test_missing_source_exits(self):
        with pytest.raises(SystemExit):
            main.parse_args(["--window-start", "2026-08-12T14:10:00+09:00"])

    def test_missing_window_start_exits(self):
        with pytest.raises(SystemExit):
            main.parse_args(["--source", "bike_station_realtime"])

    def test_force_and_backfill_together_exits(self):
        with pytest.raises(SystemExit):
            main.parse_args(
                [
                    "--source", "bike_station_realtime",
                    "--window-start", "2026-08-12T14:10:00+09:00",
                    "--force", "--backfill",
                ]
            )


class TestExitCodeFor:
    @pytest.mark.parametrize(
        "status", [RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED]
    )
    def test_ok_statuses_map_to_zero(self, status):
        assert main.exit_code_for(status) == 0

    def test_failed_maps_to_nonzero(self):
        assert main.exit_code_for(RunStatus.FAILED) != 0


class TestMain:
    @pytest.fixture
    def stub_config(self):
        return SimpleNamespace(source_id="bike_station_realtime")

    def test_happy_path_returns_zero_and_calls_pipeline(self, monkeypatch, stub_config):
        captured = {}

        def fake_load(source_id):
            captured["loaded_source_id"] = source_id
            return stub_config

        def fake_execute_window(config, window_start, *, client, force, backfill):
            captured["config"] = config
            captured["window_start"] = window_start
            captured["force"] = force
            captured["backfill"] = backfill
            return SimpleNamespace(status=RunStatus.SUCCEEDED)

        monkeypatch.setattr(main.config_loader, "load", fake_load)
        monkeypatch.setattr(main.pipeline, "execute_window", fake_execute_window)

        code = main.main(["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00"])

        assert code == 0
        assert captured["loaded_source_id"] == "bike_station_realtime"
        assert captured["config"] is stub_config
        assert captured["window_start"] == datetime(2026, 8, 12, 14, 10, tzinfo=KST)
        assert captured["force"] is False
        assert captured["backfill"] is False

    def test_failed_status_returns_nonzero(self, monkeypatch, stub_config):
        monkeypatch.setattr(main.config_loader, "load", lambda source_id: stub_config)
        monkeypatch.setattr(
            main.pipeline, "execute_window",
            lambda *a, **k: SimpleNamespace(status=RunStatus.FAILED),
        )

        code = main.main(["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00"])

        assert code != 0

    def test_configures_logging_before_loading_config(self, monkeypatch, stub_config):
        order = []

        monkeypatch.setattr(main, "configure_logging", lambda *a, **k: order.append("logging"))
        monkeypatch.setattr(
            main.config_loader, "load",
            lambda source_id: (order.append("config"), stub_config)[1],
        )
        monkeypatch.setattr(
            main.pipeline, "execute_window",
            lambda *a, **k: SimpleNamespace(status=RunStatus.SUCCEEDED),
        )

        main.main(["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00"])

        assert order == ["logging", "config"]

    def test_passes_force_and_backfill_through(self, monkeypatch, stub_config):
        captured = {}
        monkeypatch.setattr(main.config_loader, "load", lambda source_id: stub_config)

        def fake_execute_window(config, window_start, *, client, force, backfill):
            captured["force"] = force
            captured["backfill"] = backfill
            return SimpleNamespace(status=RunStatus.SUCCEEDED)

        monkeypatch.setattr(main.pipeline, "execute_window", fake_execute_window)

        main.main(
            ["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00", "--backfill"]
        )

        assert captured["force"] is False
        assert captured["backfill"] is True
