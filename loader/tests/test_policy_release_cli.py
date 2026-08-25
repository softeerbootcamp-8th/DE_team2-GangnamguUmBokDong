"""Production suite와 search CLI의 release 경계·종료 코드를 검증한다."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from evaluation import run_policy_search as search_module
from evaluation import run_policy_suite as suite_module
from evaluation.production_policy_contract import (
    PRODUCTION_POLICY_NAME,
    PRODUCTION_TARGET_DATES,
)
from evaluation.run_policy_backtest import (
    default_policy_variants,
    parse_args as parse_backtest_args,
)
from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import MAX_STOPS_PER_ROUTE


def test_backtest_default_variant_is_exact_production_policy() -> None:
    """직접 backtest 기본값도 legacy가 아니라 배포 정책을 사용한다."""
    variants = default_policy_variants((MAX_STOPS_PER_ROUTE,))

    assert len(variants) == 1
    assert variants[0].name == PRODUCTION_POLICY_NAME
    assert variants[0].max_stops_per_route == MAX_STOPS_PER_ROUTE
    assert variants[0].policy_config is DEFAULT_REBALANCE_POLICY


def test_backtest_cli_defaults_to_single_production_stop_limit() -> None:
    """직접 CLI도 명시하지 않으면 production 작업 상한 하나만 평가한다."""
    args = parse_backtest_args(
        (
            "--date",
            "2025-05-17",
            "--center",
            "hangnyeoul",
            "--rental-csv",
            "rental.csv",
            "--stock-csv",
            "stock.csv",
        )
    )

    assert args.max_stops == [MAX_STOPS_PER_ROUTE]


def test_suite_rejects_nonproduction_max_stops() -> None:
    """Suite CLI 인자로 production 경로 작업 상한을 우회할 수 없다."""
    with pytest.raises(SystemExit) as exc_info:
        suite_module.parse_args(("--max-stops", "5", "8"))

    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    "override",
    (
        ("--dates", "2025-05-17"),
        ("--center", "another-center"),
        ("--start-hour", "22"),
        ("--fleet-size", "1"),
    ),
)
def test_suite_rejects_nonproduction_evidence_scope(
    override: tuple[str, ...],
) -> None:
    """Suite CLI는 날짜·센터·시작·차량 production scope override를 거부한다."""
    with pytest.raises(SystemExit) as exc_info:
        suite_module.parse_args(override)

    assert exc_info.value.code == 2


def test_suite_passes_single_explicit_production_variant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suite 실행은 exact production variant 하나만 backtest에 전달한다."""
    captured: dict[str, Any] = {}

    def fake_backtest(**kwargs: Any) -> object:
        """Suite가 전달한 실행 인자를 저장한다."""
        captured.update(kwargs)
        return object()

    _patch_result_pipeline(
        monkeypatch,
        module=suite_module,
        tmp_path=tmp_path,
        aggregate={
            "acceptance_gate": {
                "passed": True,
                "passing_policies": [PRODUCTION_POLICY_NAME],
            }
        },
    )
    monkeypatch.setattr(suite_module, "run_policy_backtest", fake_backtest)

    exit_code = suite_module.main(("--output-dir", str(tmp_path)))

    variants = captured["policy_variants"]
    assert exit_code == 0
    assert len(variants) == 1
    assert variants[0].name == PRODUCTION_POLICY_NAME
    assert variants[0].max_stops_per_route == MAX_STOPS_PER_ROUTE
    assert variants[0].policy_config is DEFAULT_REBALANCE_POLICY
    assert captured["target_date"] == PRODUCTION_TARGET_DATES[-1]


def test_suite_returns_nonzero_when_production_release_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suite는 production acceptance 실패를 성공 종료로 숨기지 않는다."""
    _patch_result_pipeline(
        monkeypatch,
        module=suite_module,
        tmp_path=tmp_path,
        aggregate={
            "acceptance_gate": {"passed": False, "passing_policies": []}
        },
    )
    monkeypatch.setattr(suite_module, "run_policy_backtest", lambda **_: object())

    exit_code = suite_module.main(("--output-dir", str(tmp_path)))

    assert exit_code == 1


def test_search_writes_results_without_claiming_production_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Search 성공은 0으로 끝나지만 production release PASSED를 주장하지 않는다."""
    _patch_result_pipeline(
        monkeypatch,
        module=search_module,
        tmp_path=tmp_path,
        aggregate={
            "acceptance_gate": {
                "passed": True,
                "passing_policies": [PRODUCTION_POLICY_NAME],
            },
            "candidate_gate": {
                "passed": False,
                "passing_policies": [],
            },
        },
    )
    monkeypatch.setattr(search_module, "run_policy_backtest", lambda **_: object())

    exit_code = search_module.main(
        ("--dates", "2025-05-17", "--output-dir", str(tmp_path))
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Production acceptance: NOT PASSED (search-only)" in output
    assert "Production acceptance: PASSED" not in output


def _patch_result_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module: Any,
    tmp_path: Path,
    aggregate: dict[str, Any],
) -> None:
    """CLI 외부 I/O를 최소 JSON 결과와 결정적 집계로 대체한다."""
    result_path = tmp_path / "result.json"

    def fake_write_result(_: object, __: Path) -> tuple[Path, Path]:
        """Main이 다시 읽을 최소 날짜 결과를 기록한다."""
        result_path.write_text(json.dumps({"fixture": True}), encoding="utf-8")
        return result_path, tmp_path / "result.md"

    monkeypatch.setattr(module, "write_result", fake_write_result)
    monkeypatch.setattr(module, "aggregate_results", lambda _: aggregate)
    monkeypatch.setattr(module, "write_aggregate", lambda *_, **__: None)
