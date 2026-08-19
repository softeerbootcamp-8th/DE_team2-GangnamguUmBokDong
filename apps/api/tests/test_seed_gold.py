"""레거시 Gold 시드 차단 동작을 검증한다."""

import pytest
import seed_gold


def test_legacy_seed_returns_failure_without_database_access(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """구 스키마용 시드는 DB에 접근하지 않고 명확한 실패를 반환한다."""
    assert seed_gold.main() == 1

    captured = capsys.readouterr()
    assert "비활성화" in captured.err
    assert "loader/gold_cli.py" in captured.err
    assert "검증된 seed publisher" in captured.err
