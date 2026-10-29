"""``dags/blueprints.py``'s failure callback is driven here as Airflow would drive it (#2071).

Before this, ``grep _failure_callback tests/`` was empty and the function swallowed every exception:
a backend outage at failure time left the Factory Cell ``running`` and its admission unit held.
"""

from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest
from support import load_dag_module

airflow = pytest.importorskip("airflow")

from swfactory.cell_callback import DEBT_XCOM_KEY  # noqa: E402
from swfactory.recovery_accounting import CallbackDebt  # noqa: E402


class _TI:
    def __init__(self, jobs: list[dict]) -> None:
        self.task_id, self.map_index, self.try_number = "job.build", 0, 1
        self.pushed: dict[str, object] = {}
        self._jobs = jobs

    def xcom_pull(self, task_ids=None, key="return_value", map_indexes=None):
        if task_ids == "fan_out":
            return self._jobs
        return [None] if isinstance(task_ids, list) else self.pushed.get(key)

    def xcom_push(self, key: str, value: object) -> None:
        self.pushed[key] = value


def test_the_failure_callback_records_the_lost_report_instead_of_swallowing_it(monkeypatch: pytest.MonkeyPatch) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        monkeypatch.setenv("SWF_BACKEND_URL", f"http://127.0.0.1:{probe.getsockname()[1]}")
    monkeypatch.setenv("SWF_BACKEND_TOKEN", "b" * 40)
    job = {
        "cell_managed": True,
        "cell_id": "cell_555aa0b670e50aef10c1f7fd",
        "cell_epoch": 1,
        "job_idx": 0,
        "issue": "1",
    }
    ti = _TI([job])
    context = {"ti": ti, "dag_run": SimpleNamespace(run_id="swf__run"), "dag": SimpleNamespace(task_ids=["job.build"])}

    load_dag_module()._failure_callback(context)  # must not raise: Airflow's own verdict is unchanged

    assert ti.pushed.get(DEBT_XCOM_KEY) == [
        CallbackDebt("cell_555aa0b670e50aef10c1f7fd", 1, "swf__run", "job.build", "failed", attempt=1).__dict__
    ], "the undelivered failure report must be written down as debt"


def test_every_stage_task_carries_the_failure_callback() -> None:
    module = load_dag_module()
    for dag in (v for k, v in vars(module).items() if k.startswith("dag_")):
        for task in dag.tasks:
            if task.task_id.startswith("job.") and not task.task_id.startswith(("job.approve_", "job.teardown")):
                assert module._failure_callback in (task.on_failure_callback or []), (dag.dag_id, task.task_id)


def test_a_setup_that_fails_for_good_closes_the_cell_it_may_have_created(monkeypatch: pytest.MonkeyPatch) -> None:
    """teardown is ``as_teardown(setups=setup)``: with no successful setup Airflow marks it
    upstream_failed and never runs it, so setup's final-failure callback closes the cell instead,
    without re-checking accepted inputs (cleanup must not be refused by drift)."""
    from swfactory import stages

    module = load_dag_module()
    built: list[tuple[str, str, str, bool]] = []
    closed: list[str] = []

    def fake_ctx(name: str, job: dict, run_id: str, *, enforce_inputs: bool = True) -> SimpleNamespace:
        built.append((name, job["issue"], run_id, enforce_inputs))
        return SimpleNamespace(name=name)

    monkeypatch.setattr(module, "_ctx", fake_ctx)
    monkeypatch.setattr(stages, "teardown", lambda ctx: closed.append(ctx.name))
    dags = [v for k, v in vars(module).items() if k.startswith("dag_")]
    assert dags
    for dag in dags:
        callbacks = dag.get_task("job.setup").on_failure_callback or []
        assert module._failure_callback in callbacks, dag.dag_id  # the Cell is still reported failed
        (close,) = [callback for callback in callbacks if callback is not module._failure_callback]
        ti = _TI([{"issue": "7", "job_idx": 0}])
        ti.task_id = "job.setup"
        close({"ti": ti, "dag_run": SimpleNamespace(run_id="swf__run")})
        ti.map_index = 3  # no such job: nothing to close
        close({"ti": ti, "dag_run": SimpleNamespace(run_id="swf__run")})

    assert closed == [dag.dag_id for dag in dags]
    assert built == [(dag.dag_id, "7", "swf__run", False) for dag in dags]
