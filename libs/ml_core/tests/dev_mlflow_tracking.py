"""mlflow_tracking.configure()가 tracking URI/experiment을 정확히 mlflow SDK에
넘기는지 검증한다 — 실제 서버 연결은 하지 않고 mlflow.set_tracking_uri/set_experiment
호출만 monkeypatch로 가로챈다.
"""

from ml_core import mlflow_tracking


def test_configure_sets_tracking_uri_and_experiment(monkeypatch):
    calls = {}
    monkeypatch.setattr(mlflow_tracking.mlflow, "set_tracking_uri", lambda uri: calls.setdefault("uri", uri))
    monkeypatch.setattr(mlflow_tracking.mlflow, "set_experiment", lambda name: calls.setdefault("experiment", name))
    monkeypatch.setattr(mlflow_tracking, "MLFLOW_TRACKING_URI", "http://example:5000")

    mlflow_tracking.configure("bike-demand-training")

    assert calls == {"uri": "http://example:5000", "experiment": "bike-demand-training"}
