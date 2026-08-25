"""emr_orphan_reaper DAG — monthly_retrain의 setup/teardown 보장이 못 미치는
시나리오(스케줄러 자체 장애 등)까지 커버하는 독립적 EMR 정리 안전망을 검증한다.

핵심은 "재학습이 실제로 돌고 있는 클러스터는 절대 안 건드리는지"다 — EMR 스텝
활동(get_cluster_step_activity)만으로 판단하고 Airflow DAG 상태는 보지 않는다
(Airflow 3.x Task SDK가 그 경로를 지원하지 않음, 모듈 docstring 참고).
"""

from datetime import UTC, datetime, timedelta

import dags.emr_orphan_reaper as reaper_dag


def _freeze_now(monkeypatch, now: datetime) -> None:
    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(reaper_dag, "datetime", _FrozenDatetime)


def test_emr_orphan_reaper_dag_structure() -> None:
    assert reaper_dag.dag.dag_id == "emr_orphan_reaper"
    assert set(reaper_dag.dag.task_ids) == {"reap_orphan_emr_clusters"}


def test_reap_orphan_emr_clusters_never_touches_cluster_with_active_step(monkeypatch):
    """지금 활성 스텝이 있으면(=재학습이 실제로 돌고 있으면) 나이가 얼마든
    절대 종료하면 안 된다 — 시간 기반 절대 상한을 두지 않은 이유(1년치 학습이
    단일 머신에서 24시간 걸린 이력이 있음, 2026-08)를 그대로 검증한다."""
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    _freeze_now(monkeypatch, now)
    monkeypatch.setattr(
        reaper_dag,
        "list_active_emr_clusters",
        lambda: [
            {
                "id": "j-training",
                "name": "ml-monthly-retrain-rental",
                "state": "WAITING",
                "created_at": now - timedelta(hours=30),  # 예전 절대 상한(24시간)이었다면 죽었을 나이
            }
        ],
    )
    monkeypatch.setattr(
        reaper_dag, "get_cluster_step_activity", lambda cluster_id: {"has_active_step": True, "last_step_completed_at": None}
    )
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == []
    assert result["still_active"] == ["j-training"]
    assert result["reaped"] == []


def test_reap_orphan_emr_clusters_terminates_after_idle_grace_period(monkeypatch):
    """활성 스텝이 없고, 마지막 스텝이 끝난 지 유예 시간(15분)이 지났으면 종료한다."""
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    _freeze_now(monkeypatch, now)
    monkeypatch.setattr(reaper_dag, "EMR_IDLE_GRACE_MINUTES", 15)
    monkeypatch.setattr(
        reaper_dag,
        "list_active_emr_clusters",
        lambda: [
            {"id": "j-done", "name": "ml-monthly-retrain-rental", "state": "WAITING", "created_at": now - timedelta(hours=2)}
        ],
    )
    monkeypatch.setattr(
        reaper_dag,
        "get_cluster_step_activity",
        lambda cluster_id: {"has_active_step": False, "last_step_completed_at": now - timedelta(minutes=20)},
    )
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == ["j-done"]
    assert result["reaped"] == ["j-done"]


def test_reap_orphan_emr_clusters_waits_out_grace_period_before_terminating(monkeypatch):
    """유휴 상태여도 유예 시간(15분) 안에는 아직 종료하지 않는다 — 스텝 사이 짧은
    간격(리사이즈 대기 등)에서 오검출하지 않기 위함."""
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    _freeze_now(monkeypatch, now)
    monkeypatch.setattr(reaper_dag, "EMR_IDLE_GRACE_MINUTES", 15)
    monkeypatch.setattr(
        reaper_dag,
        "list_active_emr_clusters",
        lambda: [
            {"id": "j-mid", "name": "ml-monthly-retrain-return", "state": "WAITING", "created_at": now - timedelta(hours=1)}
        ],
    )
    monkeypatch.setattr(
        reaper_dag,
        "get_cluster_step_activity",
        lambda cluster_id: {"has_active_step": False, "last_step_completed_at": now - timedelta(minutes=5)},
    )
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == []
    assert result["still_within_grace"] == ["j-mid"]


def test_reap_orphan_emr_clusters_uses_creation_time_when_no_steps_yet(monkeypatch):
    """스텝이 하나도 없으면(막 생성된 직후) 클러스터 생성 시각을 유휴 기준으로 쓴다."""
    now = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
    _freeze_now(monkeypatch, now)
    monkeypatch.setattr(reaper_dag, "EMR_IDLE_GRACE_MINUTES", 15)
    monkeypatch.setattr(
        reaper_dag,
        "list_active_emr_clusters",
        lambda: [
            {
                "id": "j-new",
                "name": "ml-monthly-retrain-rental",
                "state": "STARTING",
                "created_at": now - timedelta(minutes=5),
            }
        ],
    )
    monkeypatch.setattr(
        reaper_dag, "get_cluster_step_activity", lambda cluster_id: {"has_active_step": False, "last_step_completed_at": None}
    )
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == []  # 생성된 지 5분(< 15분 유예) — 아직 종료 안 함
    assert result["still_within_grace"] == ["j-new"]


def test_reap_orphan_emr_clusters_no_op_when_nothing_active(monkeypatch):
    monkeypatch.setattr(reaper_dag, "list_active_emr_clusters", list)
    terminated = []
    monkeypatch.setattr(reaper_dag, "terminate_emr_cluster", lambda cluster_id: terminated.append(cluster_id))

    result = reaper_dag._reap_orphan_emr_clusters()

    assert terminated == []
    assert result == {"total": 0, "reaped": [], "still_active": [], "still_within_grace": []}
