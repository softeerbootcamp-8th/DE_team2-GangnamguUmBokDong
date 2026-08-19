"""rebalance CLI 입력 계약을 검증한다."""

import main
import pytest


def test_main_rejects_minute_outside_five_minute_grid():
    with pytest.raises(SystemExit) as exc_info:
        main.main(["--date", "2026-08-16", "--hour", "14", "--minute", "33"])

    assert exc_info.value.code == 2
