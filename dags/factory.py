"""Airflow DAG ``factory``: the linear software-factory pipeline with two human gates.

setup -> intent -> approve_intent -> record_intent -> spec -> plan -> approve_plan -> record_plan
-> build_and_test -> review -> deliver -> metrics ; teardown (all_done)

Loops live inside the stage functions (``swfactory.stages``), never in the DAG. ``swfactory`` is
imported only inside callables so DAG parsing needs nothing but Airflow. Every stage task builds
its context from ``dag_run.conf`` + ``Config()``; the sandbox name is derived from the run id so
retries and ``tasks clear`` re-attach to the same sandbox (``islo use`` is create-if-needed).
"""

from __future__ import annotations

import hashlib
import os
from datetime import timedelta
from typing import Any

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import DAG, Param, task

STAGES = ("intent", "spec", "plan", "build_and_test", "review", "deliver")
GATES = {"intent": "intent.md", "plan": "plan.md"}

AUTO_APPROVE = os.environ.get("SWF_APPROVE", "prompt") == "auto"
# With SWF_APPROVE=auto the gate resolves to its default after a short timeout (so
# `airflow dags test` finishes); otherwise a human has SWF_GATE_TIMEOUT_H hours to respond.
GATE_TIMEOUT = (
    timedelta(seconds=int(os.environ.get("SWF_AUTO_GATE_S", "10")))
    if AUTO_APPROVE
    else timedelta(hours=int(os.environ.get("SWF_GATE_TIMEOUT_H", "24")))
)
STAGE_TIMEOUT = timedelta(hours=int(os.environ.get("SWF_STAGE_TIMEOUT_H", "3")))


def _run_id(dag_run_id: str) -> str:
    """Stable 8-hex run id per DAG run: same sandbox name on every task and retry."""
    return hashlib.sha1(dag_run_id.encode("utf-8")).hexdigest()[:8]


def _config(conf: dict[str, Any], dag_run_id: str):
    from swfactory.config import Config

    return Config(
        issue=str(conf["issue"]),
        run_id=_run_id(dag_run_id),
        approve=os.environ.get("SWF_APPROVE", "prompt"),
    )


def _ctx(conf: dict[str, Any], dag_run_id: str):
    """Build the stage ``Ctx`` for this DAG run (all swfactory imports live here)."""
    from pathlib import Path

    from swfactory.agent import make_agent
    from swfactory.sandbox import make_sandbox
    from swfactory.scm import make_scm
    from swfactory.stages import Ctx

    cfg = _config(conf, dag_run_id)
    run_dir = Path(".factory") / cfg.run_id
    if cfg.sandbox == "local":  # one workdir per run, seeded by stages.setup()
        cfg = cfg.model_copy(update={"workdir": str(run_dir / "work")})
    base_repo = Path(cfg.workdir) if cfg.sandbox == "local" else None
    scm = make_scm(cfg, run_dir, base_repo=base_repo)
    issue = scm.fetch_issue(cfg.issue)
    return Ctx(  # contract is loaded lazily (setup() seeds the workdir first)
        cfg=cfg,
        sb=make_sandbox(cfg, issue.id),
        agent=make_agent(cfg),
        scm=scm,
        issue=issue,
        run_dir=run_dir,
    )


def _actor(responded_by_user: Any) -> str:
    """Airflow HITL ``responded_by_user`` (dict / str / None) -> approvals.json actor."""
    if isinstance(responded_by_user, dict):
        return str(responded_by_user.get("name") or responded_by_user.get("id") or "auto")
    return str(responded_by_user) if responded_by_user else "auto"


def _stage_task(name: str, *, retries: int):
    @task(task_id=name, retries=retries, execution_timeout=STAGE_TIMEOUT)
    def _run(**context: Any) -> dict:
        from swfactory import stages

        dag_run = context["dag_run"]
        ctx = _ctx(dag_run.conf, dag_run.run_id)
        return getattr(stages, name)(ctx).model_dump()

    return _run


def _record_task(gate: str):
    @task(task_id=f"record_{gate}")
    def _run(**context: Any) -> dict:
        from datetime import UTC, datetime

        from swfactory.models import Approval
        from swfactory.stages import record_approval

        response = context["ti"].xcom_pull(task_ids=f"approve_{gate}") or {}
        chosen = (response.get("chosen_options") or [ApprovalOperator.APPROVE])[0]
        approval = Approval(
            gate=gate,
            decision="approve" if chosen == ApprovalOperator.APPROVE else "reject",
            actor=_actor(response.get("responded_by_user")),
            at=datetime.now(UTC),
        )
        dag_run = context["dag_run"]
        record_approval(_ctx(dag_run.conf, dag_run.run_id), approval)
        return approval.model_dump(mode="json")

    return _run


def _approve_task(gate: str) -> ApprovalOperator:
    artifact = GATES[gate]
    return ApprovalOperator(
        task_id=f"approve_{gate}",
        subject=f"factory: approve {gate} for issue {{{{ dag_run.conf['issue'] }}}}",
        body=(
            f"Read `docs/factory/<issue>/{artifact}` in the run sandbox (the `{gate}` task log "
            f"prints it) and approve or reject. Run {{{{ dag_run.run_id }}}}."
        ),
        defaults=ApprovalOperator.APPROVE if AUTO_APPROVE else None,
        response_timeout=GATE_TIMEOUT,
        ignore_downstream_trigger_rules=True,
        fail_on_reject=False,
    )


with DAG(
    dag_id="factory",
    schedule=None,
    catchup=False,
    params={"issue": Param("", type="string", description="issue number or path to issue .md")},
    tags=["swfactory"],
    doc_md=__doc__,
) as dag:

    @task(task_id="setup", retries=2, execution_timeout=STAGE_TIMEOUT)
    def setup(**context: Any) -> dict:
        from swfactory import stages

        dag_run = context["dag_run"]
        return stages.setup(_ctx(dag_run.conf, dag_run.run_id)).model_dump()

    @task(task_id="metrics")
    def metrics(**context: Any) -> dict:
        import json

        from swfactory.config import Config

        dag_run = context["dag_run"]
        ctx = _ctx(dag_run.conf, dag_run.run_id)
        path = f"{Config.artifacts_dir(ctx.issue.id)}/metrics.json"
        return json.loads(ctx.sb.read(path)) if ctx.sb.exists(path) else {}

    @task(task_id="teardown", trigger_rule="all_done")
    def teardown(**context: Any) -> None:
        from pathlib import Path

        from swfactory.sandbox import make_sandbox
        from swfactory.scm import make_scm

        dag_run = context["dag_run"]
        cfg = _config(dag_run.conf, dag_run.run_id)
        issue = make_scm(cfg, Path(".factory") / cfg.run_id).fetch_issue(cfg.issue)
        make_sandbox(cfg, issue.id).close()  # never ensure() here: teardown must not recreate

    stage_retries = {"deliver": 2}
    stage_tasks = {name: _stage_task(name, retries=stage_retries.get(name, 0)) for name in STAGES}

    chain = [
        setup(),
        stage_tasks["intent"](),
        _approve_task("intent"),
        _record_task("intent")(),
        stage_tasks["spec"](),
        stage_tasks["plan"](),
        _approve_task("plan"),
        _record_task("plan")(),
        stage_tasks["build_and_test"](),
        stage_tasks["review"](),
        stage_tasks["deliver"](),
        metrics(),
    ]
    for upstream, downstream in zip(chain, chain[1:], strict=False):
        upstream >> downstream
    chain[-1] >> teardown().as_teardown(setups=chain[0])
