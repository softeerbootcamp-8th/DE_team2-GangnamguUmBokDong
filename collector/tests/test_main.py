"""main.py 테스트: 인자 파싱, --force/--backfill 동시 지정 차단, 종료 코드 매핑."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

import main
from manifest import RunStatus

KST = ZoneInfo("Asia/Seoul")
POI_MASTER_URI = (
    "s3://test-bucket/source_snapshot_manifest/poi_master/"
    "dt=2026-08-25/hh=00/logical=20260825T000000000000Z/"
    "revision=0000000000.json"
)
POI_MASTER_SHA256 = "a" * 64


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

    def test_poi_master_defaults_to_static_without_refs(self):
        args = main.parse_args(
            [
                "--source",
                "population_realtime",
                "--window-start",
                "2026-08-12T14:10:00+09:00",
            ]
        )

        assert args.poi_master_mode == "static"
        assert args.poi_master_manifest_uri is None
        assert args.poi_master_manifest_sha256 is None

    def test_static_mode_normalizes_airflow_empty_ref_values(self):
        """Airflow의 고정 command shape에서 빈 ref 환경변수는 미지정으로 취급한다."""
        args = main.parse_args(
            [
                "--source",
                "population_realtime",
                "--window-start",
                "2026-08-12T14:10:00+09:00",
                "--poi-master-mode",
                "static",
                "--poi-master-manifest-uri",
                "",
                "--poi-master-manifest-sha256",
                "   ",
            ]
        )

        assert args.poi_master_manifest_uri is None
        assert args.poi_master_manifest_sha256 is None

    def test_s3_poi_master_requires_both_exact_ref_fields(self):
        with pytest.raises(SystemExit):
            main.parse_args(
                [
                    "--source",
                    "population_realtime",
                    "--window-start",
                    "2026-08-12T14:10:00+09:00",
                    "--poi-master-mode",
                    "s3",
                    "--poi-master-manifest-uri",
                    POI_MASTER_URI,
                ]
            )

    @pytest.mark.parametrize(
        "ref_args",
        [
            ["--poi-master-manifest-uri", POI_MASTER_URI],
            ["--poi-master-manifest-sha256", POI_MASTER_SHA256],
            [
                "--poi-master-manifest-uri",
                POI_MASTER_URI,
                "--poi-master-manifest-sha256",
                POI_MASTER_SHA256,
            ],
        ],
    )
    def test_static_poi_master_rejects_manifest_refs(self, ref_args):
        with pytest.raises(SystemExit):
            main.parse_args(
                [
                    "--source",
                    "population_realtime",
                    "--window-start",
                    "2026-08-12T14:10:00+09:00",
                    *ref_args,
                ]
            )

    def test_non_population_source_rejects_poi_master_ref(self):
        with pytest.raises(SystemExit):
            main.parse_args(
                [
                    "--source",
                    "bike_station_realtime",
                    "--window-start",
                    "2026-08-12T14:10:00+09:00",
                    "--poi-master-mode",
                    "s3",
                    "--poi-master-manifest-uri",
                    POI_MASTER_URI,
                    "--poi-master-manifest-sha256",
                    POI_MASTER_SHA256,
                ]
            )


class TestPoiMasterConfig:
    """S3 POI Master를 한 번 읽어 frozen Collector 설정에 고정하는 계약을 검증한다."""

    def test_codes_are_validated_sorted_and_injected_without_mutating_yaml_config(
        self, monkeypatch
    ):
        config = main.config_loader.load("population_realtime")
        original_params = dict(config.adapter_params)
        calls = []
        table = pa.table({"AREA_CD": ["POI132", "POI001"]})

        def fake_read(ref, columns=None):
            calls.append((ref, columns))
            return table

        monkeypatch.setattr(main, "read_poi_master", fake_read)
        ref = main.PoiMasterRef(
            mode="s3",
            manifest_uri=POI_MASTER_URI,
            manifest_sha256=POI_MASTER_SHA256,
        )

        result = main._config_with_poi_master(config, ref)

        assert len(calls) == 1
        assert calls[0] == (ref, ["AREA_CD"])
        assert result.adapter_params["poi_codes"] == ("POI001", "POI132")
        assert config.adapter_params == original_params
        assert "poi_codes" not in config.adapter_params

    def test_config_version_combines_yaml_hash_and_manifest_hash(self, monkeypatch):
        config = main.config_loader.load("population_realtime")
        monkeypatch.setattr(
            main,
            "read_poi_master",
            lambda *_args, **_kwargs: pa.table({"AREA_CD": ["POI001"]}),
        )
        ref = main.PoiMasterRef(
            mode="s3",
            manifest_uri=POI_MASTER_URI,
            manifest_sha256=POI_MASTER_SHA256,
        )

        result = main._config_with_poi_master(config, ref)

        material = (
            f"collector_config={config.config_version}\n"
            f"poi_master_manifest_sha256={POI_MASTER_SHA256}\n"
        ).encode("ascii")
        assert result.config_version == f"sha256:{hashlib.sha256(material).hexdigest()}"
        assert result.config_version != config.config_version

    @pytest.mark.parametrize(
        "table",
        [
            pa.table({"AREA_CD": pa.array([], type=pa.string())}),
            pa.table({"AREA_CD": ["POI001", "POI001"]}),
            pa.table({"AREA_CD": ["POI1"]}),
            pa.table({"AREA_CD": ["POI１２３"]}),
            pa.table({"AREA_CD": [None]}),
            pa.table({"AREA_CD": ["POI001"], "AREA_NM": ["장소"]}),
        ],
    )
    def test_rejects_invalid_master_code_tables(self, table):
        with pytest.raises(ValueError, match="POI Master"):
            main._validated_master_poi_codes(table)


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

    @pytest.mark.parametrize(
        ("now", "expected_due"),
        [
            pytest.param(
                datetime(2026, 8, 26, 14, 9, tzinfo=KST),
                False,
                id="before-14-release-availability",
            ),
            pytest.param(
                datetime(2026, 8, 26, 14, 10, tzinfo=KST),
                True,
                id="14-release-available",
            ),
        ],
    )
    def test_short_term_due_when_release_cycle_advances(
        self, monkeypatch, capsys, now, expected_due
    ):
        """3시간 미만이어도 새 단기예보 발표본이 열리면 즉시 수집한다."""
        last = datetime(2026, 8, 26, 13, 50, tzinfo=KST)

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
            [
                "--source",
                "weather_short_term_forecast",
                "--check-due-after-seconds",
                "10800",
            ]
        )

        assert code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["due"] is expected_due
        assert payload["elapsed_seconds"] < 10800

    def test_short_term_not_due_twice_for_same_release(
        self, monkeypatch, capsys
    ):
        """14시 발표본을 받은 뒤 같은 발표 주기에서는 중복 수집하지 않는다."""
        now = datetime(2026, 8, 26, 14, 15, tzinfo=KST)
        last = datetime(2026, 8, 26, 14, 10, tzinfo=KST)

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
            [
                "--source",
                "weather_short_term_forecast",
                "--check-due-after-seconds",
                "10800",
            ]
        )

        assert code == 0
        assert json.loads(capsys.readouterr().out)["due"] is False


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

    def test_s3_mode_reads_master_once_before_pipeline(self, monkeypatch):
        config = main.config_loader.load("population_realtime")
        captured = {"read_count": 0}

        monkeypatch.setattr(main.config_loader, "load", lambda _source_id: config)

        def fake_read(ref, columns=None):
            captured["read_count"] += 1
            captured["ref"] = ref
            captured["columns"] = columns
            return pa.table({"AREA_CD": ["POI132", "POI001"]})

        def fake_execute_window(config, *_args, **_kwargs):
            captured["config"] = config
            return SimpleNamespace(status=RunStatus.SUCCEEDED)

        monkeypatch.setattr(main, "read_poi_master", fake_read)
        monkeypatch.setattr(main.pipeline, "execute_window", fake_execute_window)

        code = main.main(
            [
                "--source",
                "population_realtime",
                "--window-start",
                "2026-08-12T14:10:00+09:00",
                "--poi-master-mode",
                "s3",
                "--poi-master-manifest-uri",
                POI_MASTER_URI,
                "--poi-master-manifest-sha256",
                POI_MASTER_SHA256,
            ]
        )

        assert code == 0
        assert captured["read_count"] == 1
        assert captured["ref"].manifest_uri == POI_MASTER_URI
        assert captured["ref"].manifest_sha256 == POI_MASTER_SHA256
        assert captured["columns"] == ["AREA_CD"]
        assert captured["config"].adapter_params["poi_codes"] == (
            "POI001",
            "POI132",
        )

    def test_s3_mode_rejects_invalid_manifest_ref_before_reading_master(
        self, monkeypatch
    ):
        config = main.config_loader.load("population_realtime")
        monkeypatch.setattr(main.config_loader, "load", lambda _source_id: config)
        monkeypatch.setattr(
            main,
            "read_poi_master",
            lambda *_args, **_kwargs: pytest.fail("invalid ref를 읽으려 해서는 안 됨"),
        )

        with pytest.raises(ValueError):
            main.main(
                [
                    "--source",
                    "population_realtime",
                    "--window-start",
                    "2026-08-12T14:10:00+09:00",
                    "--poi-master-mode",
                    "s3",
                    "--poi-master-manifest-uri",
                    POI_MASTER_URI,
                    "--poi-master-manifest-sha256",
                    "NOT-A-SHA256",
                ]
            )


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
