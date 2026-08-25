"""emr_orphan_reaper DAG — monthly_retrain의 setup/teardown 보장이 못 미치는
시나리오(스케줄러 자체 장애 등)까지 커버하는 독립적 EMR 정리 안전망을 검증한다."""

from datetime import UTC, datetime, timedelta

import dags.emr_orphan_reaper as reaper_dag


def test_emr_orphan_reaper_dag_structure() -> None:
    assert reaper_dag.dag.dag_id == "emr_orphan_reaper"
    assert set(reaper_dag.dag.task_ids) == {"reap_orphan_emr_clusters"}


def test_reap_orphan_emr_clusters_terminates_only_stale_ones(monkeypatch):
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(reaper_dag, "datetime", _FrozenDatetime)
    monkeypatch.setattr(reaper_dag, "EMR_ORPHAN_MAX_AGE_HOURS", 8)
    monkeypatch.setattr(
        reaper_dag,
        "list_active_emr_clusters",
        lambda: [
            {"id": "j-stale", "name": "ml-monthly-retrain-rental", "state": "WAITING", "created_at": now - timedelta(hours=9)},
            {"id": "j-fresh", "name": "ml-monthly-retrain-return", "state": "RUNNING", "created_at": now - timedelta(hours=1)},
        ],
    )
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == ["j-stale"]
    assert result == {"total": 2, "reaped": ["j-stale"], "still_young": ["j-fresh"]}


def test_reap_orphan_emr_clusters_no_op_when_nothing_active(monkeypatch):
    monkeypatch.setattr(reaper_dag, "list_active_emr_clusters", list)
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == []
    assert result == {"total": 0, "reaped": [], "still_young": []}
