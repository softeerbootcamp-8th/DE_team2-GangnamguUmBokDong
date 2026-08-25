"""main.py 테스트: 인자 파싱, --force/--backfill 동시 지정 차단, 종료 코드 매핑."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import main
import pytest
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

    def test_check_due_after_seconds_does_not_require_window_start(self):
        args = main.parse_args(
            ["--source", "weather_ultra_short_live", "--check-due-after-seconds", "600"]
        )

        assert args.check_due_after_seconds == 600
        assert args.window_start is None


class TestExitCodeFor:
    @pytest.mark.parametrize(
        "status", [RunStatus.SUCCEEDED, RunStatus.PARTIAL, RunStatus.EMPTY, RunStatus.SKIPPED]
    )
    def test_ok_statuses_map_to_zero(self, status):
        assert main.exit_code_for(status) == 0

    def test_failed_maps_to_nonzero(self):
        assert main.exit_code_for(RunStatus.FAILED) != 0


class TestCheckDueAfterSeconds:
    """Airflow freshness gate가 파싱하는 --check-due-after-seconds JSON 출력을 검증한다."""

    def test_latest_source_config_version_uses_final_revision(self, monkeypatch):
        """Freshness 판정은 최신 authority revision의 config version을 비교한다."""
        logical = datetime(2026, 8, 24, 10, 10, tzinfo=KST)
        snapshots = (
            SimpleNamespace(manifest=SimpleNamespace(config_version="old")),
            SimpleNamespace(manifest=SimpleNamespace(config_version="current")),
        )
        monkeypatch.setattr(
            main.manifest_module,
            "load_source_snapshots",
            lambda source_id, logical_dttm: snapshots,
        )

        assert main._latest_source_uses_config(
            "weather_ultra_short_live", logical, "current"
        )

    def test_due_true_when_never_collected(self, monkeypatch, capsys):
        monkeypatch.setattr(
            main.storage, "latest_source_snapshot_logical_dttm", lambda *a, **k: None
        )

        code = main.main(
            ["--source", "weather_ultra_short_live", "--check-due-after-seconds", "600"]
        )

        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload == {
            "source_id": "weather_ultra_short_live",
            "due": True,
            "last_logical_dttm": None,
            "elapsed_seconds": None,
        }

    def test_due_false_when_within_threshold(self, monkeypatch, capsys):
        """현재 config authority가 아직 신선하면 수집을 건너뛴다."""
        now = datetime(2026, 8, 24, 10, 15, tzinfo=KST)
        last = datetime(2026, 8, 24, 10, 12, tzinfo=KST)

        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        monkeypatch.setattr(main, "datetime", _FixedDatetime)
        monkeypatch.setattr(
            main.storage, "latest_source_snapshot_logical_dttm", lambda *a, **k: last
        )
        monkeypatch.setattr(main, "_latest_source_uses_config", lambda *a, **k: True)

        code = main.main(
            ["--source", "weather_ultra_short_live", "--check-due-after-seconds", "600"]
        )

        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["due"] is False
        assert payload["elapsed_seconds"] == 180.0
        assert payload["last_logical_dttm"] == last.isoformat()

    def test_due_true_once_threshold_elapsed(self, monkeypatch, capsys):
        """현재 config authority도 freshness 시간이 지나면 다시 수집한다."""
        now = datetime(2026, 8, 24, 10, 25, tzinfo=KST)
        last = datetime(2026, 8, 24, 10, 12, tzinfo=KST)

        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        monkeypatch.setattr(main, "datetime", _FixedDatetime)
        monkeypatch.setattr(
            main.storage, "latest_source_snapshot_logical_dttm", lambda *a, **k: last
        )

        code = main.main(
            ["--source", "weather_ultra_short_live", "--check-due-after-seconds", "600"]
        )

        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["due"] is True

    def test_due_true_when_latest_authority_uses_old_config(
        self, monkeypatch, capsys
    ):
        """YAML 배포 직후에는 신선한 옛 authority라도 새 설정으로 다시 수집한다."""
        now = datetime(2026, 8, 24, 10, 15, tzinfo=KST)
        last = datetime(2026, 8, 24, 10, 12, tzinfo=KST)

        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                """고정된 KST 현재 시각을 반환한다."""
                return now

        monkeypatch.setattr(main, "datetime", _FixedDatetime)
        monkeypatch.setattr(
            main.storage, "latest_source_snapshot_logical_dttm", lambda *a, **k: last
        )
        monkeypatch.setattr(main, "_latest_source_uses_config", lambda *a, **k: False)

        code = main.main(
            [
                "--source",
                "weather_ultra_short_live",
                "--check-due-after-seconds",
                "600",
            ]
        )

        assert code == 0
        assert json.loads(capsys.readouterr().out)["due"] is True


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


class TestHttpTimeout:
    """httpx 기본 timeout은 5초인데 서울 API의 실측 페이지 지연은 최대 7.19초였다.
    기본값을 그대로 쓰면 느린 시점에 페이지마다 ReadTimeout으로 5초를 버리고
    라운드 재시도(15s/30s 대기)로 넘어가 fetch 예산을 잠식한다."""

    @pytest.fixture
    def stub_config(self):
        return SimpleNamespace(source_id="bike_station_realtime")

    def test_client_timeout_is_explicit_and_longer_than_httpx_default(self, monkeypatch, stub_config):
        captured = {}

        class SpyClient:
            def __init__(self, *args, **kwargs):
                captured["timeout"] = kwargs.get("timeout")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(main.config_loader, "load", lambda source_id: stub_config)
        monkeypatch.setattr(main.httpx, "Client", SpyClient)
        monkeypatch.setattr(
            main.pipeline, "execute_window",
            lambda *a, **k: SimpleNamespace(status=RunStatus.SUCCEEDED),
        )

        main.main(["--source", "bike_station_realtime", "--window-start", "2026-08-12T14:10:00+09:00"])

        timeout = captured["timeout"]
        assert timeout is not None, "timeout을 명시하지 않으면 httpx 기본값 5초가 적용된다"
        assert timeout.read > 5.0
        assert timeout.connect is not None
