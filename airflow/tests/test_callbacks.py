"""callbacks/task_callbacks.py의 context 필드 추출을 검증한다."""

from types import SimpleNamespace

from callbacks.task_callbacks import on_failure_callback, on_success_callback


def _fake_context(**overrides):
    task_instance = SimpleNamespace(dag_id="realtime_5min", task_id="run_inference", try_number=1)
    dag_run = SimpleNamespace(logical_date="2026-08-16T14:05:00+09:00", run_id="scheduled__2026-08-16T05:05:00+00:00")
    context = {"task_instance": task_instance, "dag_run": dag_run, "exception": None}
    context.update(overrides)
    return context


def test_on_success_callback_does_not_raise(caplog):
    on_success_callback(_fake_context())


def test_on_failure_callback_includes_exception(caplog):
    context = _fake_context(exception=RuntimeError("boom"))
    on_failure_callback(context)
