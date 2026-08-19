"""inference 설정이 공유 학습·서빙 계약을 따르는지 검증한다."""

from datetime import date

from inference import config


def test_default_grid_is_five_minutes():
    """학습 feature와 5분 Airflow 추론이 같은 grid를 사용해야 한다."""
    assert config.GRID_TICK_MINUTES == 5
    assert config.ROLLING_TICK_MINUTES == 5


def test_batch_cli_default_date_is_an_actual_test_partition():
    """배치 CLI 기본일이 학습 train 날짜를 test처럼 조회하면 안 된다."""
    start = date.fromisoformat(config.TEST_START)
    end = date.fromisoformat(config.TEST_END)

    assert start == end
    assert start == date(2025, 6, 17)
