"""Serving production CLI의 exact ref·backend·stdout 계약을 검증한다."""

from __future__ import annotations

import io
import sys
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import serving_cli
from core.gold_publication import ContractViolation, PublicationOutcome


class _CatalogClient:
    """Catalog constructor가 요구하는 최소 injected S3 client다."""

    def list_objects_v2(self, **_kwargs):
        """빈 LIST page를 반환한다."""
        return {"Contents": (), "IsTruncated": False}

    def get_object(self, **_kwargs):
        """이 fixture에서 호출되면 실패한다."""
        raise AssertionError("unexpected GET")


def test_inference_eligibility_excludes_known_master_quality_errors() -> None:
    """결측 grid 같은 알려진 master 품질 문제는 expected 후보에서 격리한다."""
    table = pa.table(
        {
            "station_id": ["ST-1", "ST-2"],
            "station_no": [1, 2],
            "capacity": [10, 10],
            "lat": [37.5, 37.5],
            "lon": [127.0, 127.0],
            "grid_id": ["GRID-1", None],
        }
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)

    class _Body:
        """Boto streaming body의 최소 read 계약이다."""

        def read(self):
            """Fixture parquet bytes를 반환한다."""
            return sink.getvalue()

    class _Client:
        """Enriched master 하나를 반환하는 S3 fixture다."""

        def list_objects_v2(self, **_kwargs):
            """최신 enriched parquet key를 반환한다."""
            return {
                "Contents": [
                    {
                        "Key": "silver/station_master_enriched/"
                        "dt=2026-08-22/hh=00/0035.parquet"
                    }
                ],
                "IsTruncated": False,
            }

        def get_object(self, **_kwargs):
            """Fixture parquet body를 반환한다."""
            return {"Body": _Body()}

    eligible, excluded = serving_cli._load_inference_eligible_station_ids(
        _Client(), "fixture"
    )

    assert eligible == ("ST-1",)
    assert excluded[0][0] == "ST-2"
    assert "grid_id 값이 유효하지 않음" in excluded[0][1]


def test_prepare_pins_support_refs_with_same_client_and_bucket(monkeypatch) -> None:
    """운영자가 support ref를 고르지 않고 경량 release snapshot에서 가져온다."""
    client = _CatalogClient()
    store = object()

    class _SourceCatalog:
        """Prepare가 요청한 source selection을 기록하는 catalog다."""

        def __init__(self) -> None:
            """빈 source selection 기록을 만든다."""
            self.calls: list[tuple[str, str]] = []

        def latest_at_or_before(self, source_id, _logical, *, lookback):
            """Latest source selection을 고정 fixture로 반환한다."""
            self.calls.append(("latest", source_id))
            assert lookback.total_seconds() > 0
            return f"latest:{source_id}"

        def exact_window(self, source_id, _logical):
            """Exact source selection을 고정 fixture로 반환한다."""
            self.calls.append(("exact", source_id))
            return f"exact:{source_id}"

    source_catalog = _SourceCatalog()
    rental_ref = object()
    return_ref = object()
    pinned = SimpleNamespace(
        rental_model=SimpleNamespace(support_sta_ids=rental_ref),
        return_model=SimpleNamespace(support_sta_ids=return_ref),
    )
    captured = {}

    def load_current(*, object_store, pointer_store):
        """Pointer store가 runtime client/bucket을 그대로 쓰는지 검증한다."""
        assert object_store is store
        assert pointer_store._client is client
        assert pointer_store._bucket == "fixture"
        return pinned

    def prepare_plan(connection, object_store, **kwargs):
        """Prepare publisher 호출 인자를 기록하고 plan artifact를 반환한다."""
        assert connection == "connection"
        assert object_store is store
        captured.update(kwargs)
        return SimpleNamespace(
            uri="s3://fixture/gold_publication/p.json", byte_sha256="a" * 64
        )

    monkeypatch.setenv("GOLD_STATION_MASTER_LOOKBACK_HOURS", "168")
    monkeypatch.setenv("GOLD_STATION_REALTIME_LOOKBACK_HOURS", "24")
    monkeypatch.setattr(
        serving_cli,
        "_runtime",
        lambda: ("fixture", client, store, source_catalog),
    )
    monkeypatch.setattr(
        serving_cli,
        "load_current_serving_release_for_plan",
        load_current,
    )
    monkeypatch.setattr(
        serving_cli,
        "_load_inference_eligible_station_ids",
        lambda _client, _bucket: (("ST-1",), ()),
    )
    monkeypatch.setattr(serving_cli, "prepare_serving_plan", prepare_plan)
    monkeypatch.setattr(
        serving_cli,
        "get_connection",
        lambda: nullcontext("connection"),
    )

    result = serving_cli.prepare(datetime(2026, 8, 20, 1, 25, tzinfo=UTC))

    assert result == {
        "plan": {
            "byte_sha256": "a" * 64,
            "uri": "s3://fixture/gold_publication/p.json",
        }
    }
    assert captured["rental_support_sta_ids"] is rental_ref
    assert captured["return_support_sta_ids"] is return_ref
    assert captured["inference_eligible_sta_ids"] == ("ST-1",)


def test_finalize_returns_only_four_exact_refs(monkeypatch) -> None:
    """Finalize JSON은 downstream에 필요한 네 publication URI·SHA만 노출한다."""
    client = _CatalogClient()
    store = object()
    evidence = tuple(
        SimpleNamespace(
            manifest=SimpleNamespace(publication_key=key, sha256=character * 64),
            manifest_uri=f"s3://fixture/gold_publication/{key}.json",
        )
        for key, character in (
            ("station", "a"),
            ("station_demand_forecast", "b"),
            ("station_stock", "c"),
            ("weather_forecast", "d"),
        )
    )
    execution = SimpleNamespace(
        result=SimpleNamespace(outcome=PublicationOutcome.PUBLISHED),
        evidence=evidence,
    )
    captured = {}

    def publish(connection, object_store, **kwargs):
        """Final publisher가 same runtime catalogs를 받는지 기록한다."""
        assert connection == "connection"
        assert object_store is store
        captured.update(kwargs)
        return execution

    monkeypatch.setattr(
        serving_cli,
        "_runtime",
        lambda: ("fixture", client, store, object()),
    )
    monkeypatch.setattr(serving_cli, "publish_serving_plan", publish)
    monkeypatch.setattr(
        serving_cli,
        "get_connection",
        lambda: nullcontext("connection"),
    )

    result = serving_cli.finalize(
        plan_uri="s3://fixture/gold_publication/plan.json",
        plan_sha256="1" * 64,
        inference_uri="s3://fixture/gold_publication/inference.json",
        inference_sha256="2" * 64,
    )

    assert set(result) == {
        "station",
        "station_demand_forecast",
        "station_stock",
        "weather_forecast",
    }
    assert captured["inference_catalog"]._client is client
    assert captured["source_catalog"] is not None


def test_main_writes_compact_json_last_line_and_redirects_logs(
    monkeypatch,
    capsys,
) -> None:
    """Internal stdout은 stderr로 보내고 stdout에는 compact JSON 한 줄만 남긴다."""

    def fake_prepare(_logical, **_kwargs):
        """Legacy 내부 print를 흉내 내고 plan ref를 반환한다."""
        print("internal-log")
        return {"plan": {"byte_sha256": "a" * 64, "uri": "s3://fixture/p"}}

    monkeypatch.setattr(serving_cli, "prepare", fake_prepare)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "serving_cli.py",
            "prepare",
            "--logical-dttm",
            "2026-08-20T10:25:00+09:00",
        ],
    )

    assert serving_cli.main() == 0
    output = capsys.readouterr()
    assert output.out == (
        '{"plan":{"byte_sha256":"' + "a" * 64 + '","uri":"s3://fixture/p"}}\n'
    )
    assert "internal-log" in output.err


def test_main_maps_stale_to_nonzero(monkeypatch, capsys) -> None:
    """STALE final을 성공 XCom으로 숨기지 않고 command failure로 반환한다."""

    def stale(**_kwargs):
        """Final source drift를 흉내 낸다."""
        raise ContractViolation("STALE serving finalize")

    monkeypatch.setattr(serving_cli, "finalize", stale)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "serving_cli.py",
            "finalize",
            "--plan-uri",
            "s3://fixture/p",
            "--plan-sha256",
            "a" * 64,
            "--inference-uri",
            "s3://fixture/i",
            "--inference-sha256",
            "b" * 64,
        ],
    )

    assert serving_cli.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "STALE" in output.err


def test_missing_bucket_and_cross_bucket_refs_fail_closed(monkeypatch) -> None:
    """필수 bucket이 없거나 ref bucket이 다르면 S3 client보다 먼저 거부한다."""
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(ContractViolation, match="S3_BUCKET"):
        serving_cli._required_bucket()
    with pytest.raises(ContractViolation, match="S3_BUCKET"):
        serving_cli._require_uri_bucket(
            "s3://other/gold_publication/p.json",
            "fixture",
            "plan",
        )


def test_urgency_subcommand_passes_all_exact_release_refs(monkeypatch, capsys) -> None:
    """CLI parser가 station·demand·stock URI/SHA 쌍을 누락 없이 전달한다."""
    captured = {}

    def fake_urgency(**kwargs):
        """Parsed subcommand 인자를 기록하고 urgency ref를 반환한다."""
        captured.update(kwargs)
        return {"station_urgency": {"uri": "s3://fixture/u", "byte_sha256": "f" * 64}}

    monkeypatch.setattr(serving_cli, "urgency", fake_urgency)
    arguments = ["serving_cli.py", "urgency"]
    for key, character in (("station", "a"), ("demand", "b"), ("stock", "c")):
        arguments.extend(
            [
                f"--{key}-uri",
                f"s3://fixture/{key}",
                f"--{key}-sha256",
                character * 64,
            ]
        )
    monkeypatch.setattr(sys, "argv", arguments)

    assert serving_cli.main() == 0
    assert captured == {
        "station_uri": "s3://fixture/station",
        "station_sha256": "a" * 64,
        "demand_uri": "s3://fixture/demand",
        "demand_sha256": "b" * 64,
        "stock_uri": "s3://fixture/stock",
        "stock_sha256": "c" * 64,
    }
    assert capsys.readouterr().out.startswith('{"station_urgency":')
