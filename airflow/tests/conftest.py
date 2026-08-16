"""테스트 공용 픽스처. 실제 스케줄 DAG를 만들지 않고 orchestration/*.py의 태스크
빌더를 단독으로 검증할 때 쓰는 최소 DAG를 제공한다.
"""

import pendulum
import pytest
from airflow import DAG


@pytest.fixture
def dag():
    with DAG(
        dag_id="test_dag",
        schedule=None,
        start_date=pendulum.datetime(2026, 8, 16, tz="Asia/Seoul"),
    ) as d:
        yield d
