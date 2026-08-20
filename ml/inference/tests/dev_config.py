"""inference 설정이 공유 학습·서빙 계약을 따르는지 검증한다."""

from datetime import date

from inference import config


def test_default_model_grid_and_serving_cadence_are_separate():
    """기본 모델은 20분 grid로 학습하지만 Airflow 추론은 5분마다 실행한다."""
    assert config.GRID_TICK_MINUTES == 20
    assert config.ROLLING_TICK_MINUTES == 20
    assert config.SERVING_TICK_MINUTES == 5


def test_batch_cli_default_date_is_an_actual_test_partition():
    """배치 CLI 기본일이 학습 train 날짜를 test처럼 조회하면 안 된다."""
    start = date.fromisoformat(config.TEST_START)
    end = date.fromisoformat(config.TEST_END)

    assert start == end
    assert start == date(2025, 6, 17)
