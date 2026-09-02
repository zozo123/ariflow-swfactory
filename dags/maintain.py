"""Airflow DAG ``maintain``: nightly band check over committed run metrics + orphan sandbox sweep.

Detection is deterministic (``swfactory.maintain.detect``); the model is only invoked at the
diagnose/propose tiers, read-only, inside an islo sandbox. ``swfactory`` is imported inside the
callables so DAG parsing needs nothing but Airflow.
"""

from __future__ import annotations

import os
from typing import Any

from airflow.sdk import DAG, task

with DAG(
    dag_id="maintain",
    schedule="@daily",
    catchup=False,
    tags=["swfactory"],
    doc_md=__doc__,
) as dag:

    @task(task_id="check_bands", retries=1)
    def check_bands(**context: Any) -> list[dict]:
        from pathlib import Path

        from swfactory import maintain
        from swfactory.agent import make_agent
        from swfactory.config import Config
        from swfactory.sandbox import make_sandbox
        from swfactory.scm import make_scm

        cfg = Config(issue="maintain", run_id=context["dag_run"].run_id[-8:].replace(":", "-"))
        scm = make_scm(cfg, Path(".factory") / f"maintain-{cfg.run_id}")
        agent = make_agent(cfg) if cfg.agent == "claude" else None
        sb = make_sandbox(cfg, "maintain") if agent else None
        try:
            breaches = maintain.run(
                cfg,
                scm=scm,
                agent=agent,
                sb=sb,
                bands_path=Path(os.environ.get("SWF_BANDS", "bands.yaml")),
            )
        finally:
            if sb is not None:
                sb.close()
        return [b.model_dump() for b in breaches]

    @task(task_id="sweep_sandboxes", trigger_rule="all_done")
    def sweep_sandboxes() -> list[str]:
        from swfactory import maintain
        from swfactory.config import Config

        return maintain.sweep_sandboxes(Config(issue="maintain").sandbox_ttl_s)

    check_bands() >> sweep_sandboxes()
