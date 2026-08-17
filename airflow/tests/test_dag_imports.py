"""4개 DAG 모듈이 문법/의존성 에러 없이 로드되는지 확인한다."""

import dags.daily_population_and_events as daily_dag
import dags.realtime_5min as realtime_5min_dag
import dags.weather_3h as weather_3h_dag
import dags.weather_10min as weather_10min_dag


def test_realtime_5min_dag_id():
    assert realtime_5min_dag.dag.dag_id == "realtime_5min"


def test_weather_10min_dag_id():
    assert weather_10min_dag.dag.dag_id == "weather_10min"


def test_weather_3h_dag_id():
    assert weather_3h_dag.dag.dag_id == "weather_3h"


def test_daily_population_and_events_dag_id():
    assert daily_dag.dag.dag_id == "daily_population_and_events"
