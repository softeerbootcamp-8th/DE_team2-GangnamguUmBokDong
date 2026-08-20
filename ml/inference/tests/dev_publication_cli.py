"""Inference publication CLI의 plan ref·backend·JSON stdout 계약을 검증한다."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from core.gold_publication import ContractViolation, build_id_set
from core.inference_snapshot import ServingPlanRef

from inference import publication_cli


class _Client:
    """Catalog constructor와 object store가 공유할 최소 S3 client다."""

    def list_objects_v2(self, **_kwargs):
        """빈 catalog page를 반환한다."""
        return {"Contents": (), "IsTruncated": False}

    def get_object(self, **_kwargs):
        """이 fixture에서 unexpected exact GET을 거부한다."""
        raise AssertionError("unexpected GET")


def test_run_exact_reads_plan_and_shares_injected_backend(monkeypatch) -> None:
    """Plan extractor·producer·pointer·catalog가 같은 client/store/bucket을 사용한다."""
    client = _Client()
    inputs = SimpleNamespace(
        logical_dttm=object(),
        station_dependency=object(),
        serving_plan=ServingPlanRef(
            byte_sha256="a" * 64,
            uri=(
                "s3://fixture/gold_publication/serving-plan/plans/"
                + "sha256="
                + "a" * 64
                + ".json"
            ),
        ),
        expected_sta_ids=build_id_set(("ST-1",)),
        object_base_uri="s3://fixture/gold_publication",
    )
    captured = {}

    def read_inputs(object_store, *, plan_uri, plan_sha256):
        """Plan ref와 exact object store를 기록한다."""
        captured["object_store"] = object_store
        captured["plan"] = (plan_uri, plan_sha256)
        return inputs

    def publish(**kwargs):
        """Producer가 same backend adapters를 받는지 검증하고 결과를 반환한다."""
        captured["publish"] = kwargs
        assert kwargs["object_store"] is captured["object_store"]
        assert kwargs["revision_catalog"]._client is client
        assert kwargs["pointer_store"]._client is client
        assert kwargs["pointer_store"]._bucket == "fixture"
        return SimpleNamespace(
            manifest_uri="s3://fixture/gold_publication/inference/manifests/m.json",
            manifest_sha256="b" * 64,
        )

    monkeypatch.setenv("S3_BUCKET", "fixture")
    monkeypatch.setattr(publication_cli, "_s3_client", lambda: client)
    monkeypatch.setattr(
        publication_cli,
        "read_serving_plan_inference_inputs",
        read_inputs,
    )
    monkeypatch.setattr(publication_cli, "run_and_publish_inference", publish)
    plan_uri = inputs.serving_plan.uri

    result = publication_cli.run(plan_uri=plan_uri, plan_sha256="a" * 64)

    assert captured["plan"] == (plan_uri, "a" * 64)
    assert result == {
        "inference": {
            "byte_sha256": "b" * 64,
            "uri": "s3://fixture/gold_publication/inference/manifests/m.json",
        }
    }


def test_main_prints_only_compact_json_and_redirects_internal_stdout(
    monkeypatch,
    capsys,
) -> None:
    """Airflow XCom용 stdout은 JSON 한 줄이고 producer print는 stderr로 이동한다."""

    def fake_run(**_kwargs):
        """내부 log를 흉내 내고 inference ref를 반환한다."""
        print("predictor-log")
        return {"inference": {"byte_sha256": "b" * 64, "uri": "s3://fixture/i"}}

    monkeypatch.setattr(publication_cli, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publication_cli.py",
            "--plan-uri",
            "s3://fixture/p",
            "--plan-sha256",
            "a" * 64,
        ],
    )

    assert publication_cli.main() == 0
    output = capsys.readouterr()
    assert output.out == (
        '{"inference":{"byte_sha256":"' + "b" * 64 + '","uri":"s3://fixture/i"}}\n'
    )
    assert "predictor-log" in output.err


def test_missing_bucket_and_cross_bucket_plan_fail_closed(monkeypatch) -> None:
    """S3_BUCKET 부재와 다른 bucket plan을 client creation 전에 거부한다."""
    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(ContractViolation, match="S3_BUCKET"):
        publication_cli._required_bucket()
    with pytest.raises(ContractViolation, match="S3_BUCKET"):
        publication_cli._require_uri_bucket(
            "s3://other/gold_publication/p.json",
            "fixture",
            "plan",
        )


def test_main_failure_has_no_json_stdout(monkeypatch, capsys) -> None:
    """Plan drift나 producer 실패를 nonzero로 반환하고 잘못된 XCom을 만들지 않는다."""

    def fail(**_kwargs):
        """Plan/pointer mismatch를 흉내 낸다."""
        raise ContractViolation("plan mismatch")

    monkeypatch.setattr(publication_cli, "run", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "publication_cli.py",
            "--plan-uri",
            "s3://fixture/p",
            "--plan-sha256",
            "a" * 64,
        ],
    )

    assert publication_cli.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "plan mismatch" in output.err
