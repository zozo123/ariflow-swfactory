"""``dags/blueprints.py``'s failure callback is driven here as Airflow would drive it (#2071).

Before this, ``grep _failure_callback tests/`` was empty and the function swallowed every exception:
a backend outage at failure time left the Factory Cell ``running`` and its admission unit held.
"""

from __future__ import annotations

import importlib.util
import socket
from pathlib import Path
from types import SimpleNamespace

import pytest

airflow = pytest.importorskip("airflow")

from swfactory.cell_callback import DEBT_XCOM_KEY  # noqa: E402
from swfactory.recovery_accounting import CallbackDebt  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


def _blueprints():
    spec = importlib.util.spec_from_file_location("swf_dag_blueprints_callbacks", REPO / "dags" / "blueprints.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    job = {"cell_managed": True, "cell_id": "cell_abc", "cell_epoch": 1, "job_idx": 0, "issue": "1"}
    ti = _TI([job])
    context = {"ti": ti, "dag_run": SimpleNamespace(run_id="swf__run"), "dag": SimpleNamespace(task_ids=["job.build"])}

    _blueprints()._failure_callback(context)  # must not raise: Airflow's own verdict is unchanged

    assert ti.pushed.get(DEBT_XCOM_KEY) == [
        CallbackDebt("cell_abc", 1, "swf__run", "job.build", "failed", attempt=1).__dict__
    ], "the undelivered failure report must be written down as debt"


def test_every_stage_task_carries_the_failure_callback() -> None:
    module = _blueprints()
    for dag in (v for k, v in vars(module).items() if k.startswith("dag_")):
        for task in dag.tasks:
            if task.task_id.startswith("job.") and not task.task_id.startswith(("job.approve_", "job.teardown")):
                assert module._failure_callback in (task.on_failure_callback or []), (dag.dag_id, task.task_id)
