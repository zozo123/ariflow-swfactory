"""Airflow DAG ``maintain``: band check over committed run metrics + orphan sandbox sweep.

Runs nightly (03:00 UTC) *and* whenever any blueprint's ``deliver`` task publishes its
``swf.metrics.<blueprint>`` asset, so bands are checked right after every delivery. Detection is
deterministic (``swfactory.maintain.detect``); the model is only invoked at the diagnose/propose
tiers, read-only, inside a sandbox. ``swfactory`` is imported inside the callables so DAG parsing
needs nothing but Airflow (blueprint names come from stdlib ``tomllib``).

Metrics are read from a checkout of the target: ``$SWF_MAINTAIN_ROOT`` when set, else a shallow
clone of the target's base branch made for the task (workers do not run from a checkout).
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import tomllib
from pathlib import Path
from typing import Any

from airflow.sdk import DAG, Asset, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

FACTORY_ROOT = Path(__file__).resolve().parent.parent
BLUEPRINTS_DIR = FACTORY_ROOT / "blueprints"
NIGHTLY = CronTriggerTimetable("0 3 * * *", timezone="UTC")


def _blueprint_names(root: Path = BLUEPRINTS_DIR) -> list[str]:
    names = []
    for path in sorted(root.glob("*.toml")):
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        names.append(data.get("blueprint", {}).get("name") or path.stem)
    return names


def run_id_for(dag_run_id: str) -> str:
    """Stable 8-hex run id per DAG run. Airflow run ids end in an ISO offset (``+00:00``), which
    ``Config.sandbox_name`` rejects, so the id is hashed rather than sliced."""
    return hashlib.sha1(dag_run_id.encode()).hexdigest()[:8]


_assets = [Asset(name=f"swf.metrics.{n}") for n in _blueprint_names()]

with DAG(
    dag_id="maintain",
    schedule=AssetOrTimeSchedule(timetable=NIGHTLY, assets=_assets) if _assets else NIGHTLY,
    catchup=False,
    tags=["swfactory"],
    doc_md=__doc__,
) as dag:

    @task(task_id="check_bands", retries=1)
    def check_bands(**context: Any) -> list[dict]:
        from swfactory import maintain
        from swfactory.agent import make_agent
        from swfactory.config import Config
        from swfactory.sandbox import make_sandbox
        from swfactory.scm import make_scm

        run_id = run_id_for(context["dag_run"].run_id)
        cfg = Config(issue="maintain", run_id=run_id)
        scm = make_scm(cfg, Path(".factory") / f"maintain-{run_id}")
        agent = make_agent(cfg) if cfg.agent == "claude" else None
        sb = make_sandbox(cfg, "maintain") if agent else None
        bands_path = Path(os.environ.get("SWF_BANDS") or FACTORY_ROOT / "bands.yaml")
        try:
            with tempfile.TemporaryDirectory(prefix="swf-maintain-") as scratch:
                breaches = maintain.run(
                    cfg,
                    scm=scm,
                    agent=agent,
                    sb=sb,
                    bands_path=bands_path,
                    root=maintain.metrics_root(cfg, Path(scratch)),
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
