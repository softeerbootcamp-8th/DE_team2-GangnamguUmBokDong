"""DAG 모듈이 문법/의존성 에러 없이 로드되는지 확인한다."""

import dags.nowcasting as nowcasting_dag


class TestNowcastingDag:
    def test_dag_id(self):
        assert nowcasting_dag.dag.dag_id == "nowcasting"

    def test_has_single_estimate_task(self):
        task_ids = [t.task_id for t in nowcasting_dag.dag.tasks]
        assert task_ids == ["estimate_living_population"]

    def test_task_bash_command_targets_nowcasting_cli(self):
        task = nowcasting_dag.dag.get_task("estimate_living_population")
        assert "seoul-pop-nowcasting" in task.bash_command
        assert "main.py estimate" in task.bash_command
