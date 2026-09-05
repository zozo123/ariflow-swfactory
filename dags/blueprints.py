"""One Airflow DAG per ``blueprints/*.toml``: the software-factory line as a mapped task group.

For every blueprint file this module emits ``DAG(dag_id=<blueprint.name>)``::

    fan_out -> job[ setup -> <stage> (-> approve_<stage> -> record_<stage>)* ... -> deliver
                    -> metrics ; teardown ]            (job = one (issue x target), mapped)

``fan_out`` turns ``dag_run.conf`` (``{"issues": [...]}``, ``{"issue": N}`` accepted) into jobs
and the ``job`` task group is expanded over them, so one issue can be applied to N target repos
with one addressable approval per (issue, target). Loops live inside the stage functions
(``swfactory.stages``), never in the DAG.

Parse time reads the TOML *shape* only with stdlib ``tomllib`` (name, trigger, stage order, gates,
limits); ``swfactory`` is imported only inside task callables so DAG parsing needs nothing but
Airflow. Every task rebuilds its ``Ctx`` from the job + ``Blueprint.load(name)``; the run id is
derived from the DAG run id and job index so retries and ``tasks clear`` re-attach to the same
sandbox (``islo use`` is create-if-needed).

Gates are ``GateOperator`` (an ``ApprovalOperator`` that never skips on its own): the response
lands in XCom whatever the decision, ``record_<stage>`` writes it to ``approvals.json`` and, on
Reject, raises ``AirflowSkipException``. The work stages then skip, but ``deliver`` and
``metrics`` run with ``trigger_rule="none_failed"`` (and ``teardown`` as a teardown), so the
refusal and its actor are committed and published exactly like an approval.

``airflow dags test`` never resolves HITL tasks: use ``--mark-success-pattern 'job\\.approve_.*'``.
``SWF_APPROVE=auto`` (parse-time env) makes every gate default to Approve after its timeout.
"""

from __future__ import annotations

import os
import tomllib
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.providers.standard.operators.hitl import ApprovalOperator, HITLOperator
from airflow.sdk import DAG, Asset, Param, get_parsing_context, task, task_group

if TYPE_CHECKING:
    from airflow.sdk import Context

FACTORY_ROOT = Path(__file__).resolve().parent.parent
BLUEPRINTS_DIR = FACTORY_ROOT / "blueprints"
GROUP_ID = "job"
STAGE_RETRIES = {"deliver": 2}
APPROVE_ENV_AUTO = os.environ.get("SWF_APPROVE") == "auto"


# ---------------------------------------------------------------- parse-time shape (tomllib only)


def read_shape(path: Path) -> dict[str, Any]:
    """The subset of a blueprint the DAG structure depends on. Validation happens in tasks."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    trigger = data.get("trigger", {})
    limits = data.get("limits", {})
    return {
        "name": data.get("blueprint", {}).get("name") or path.stem,
        "cron": trigger.get("cron") if trigger.get("kind") == "cron" else None,
        "order": list(data["stages"]["order"]),
        "gates": {g["after"]: g for g in data.get("gates", [])},
        "stage_timeout": timedelta(hours=int(limits.get("stage_timeout_h", 3))),
        "max_parallel_jobs": int(limits.get("max_parallel_jobs", 4)),
    }


# ---------------------------------------------------------------- runtime wiring (swfactory)


def run_id_for(dag_run_id: str, job_idx: int) -> str:
    """Stable 8-hex run id per (DAG run, job): same sandbox name on every task and retry.

    ``swfactory.runtime.run_id_for``, imported inside the function so DAG parsing stays
    swfactory-free; ``tests/test_dag_smoke.py`` calls it to locate the run dir.
    """
    from swfactory.runtime import run_id_for as _impl

    return _impl(dag_run_id, job_idx)


def _ctx(name: str, job: dict[str, Any], dag_run_id: str):
    """The stage ``Ctx`` for one job — the same wiring ``swfactory run`` uses
    (``swfactory.runtime.build_ctx``); all swfactory imports live inside the task callables."""
    from swfactory.blueprint import load
    from swfactory.runtime import build_ctx

    return build_ctx(load(name), job, run_id=run_id_for(dag_run_id, int(job["job_idx"])))


def _actor(responded_by_user: Any) -> str:
    """Airflow HITL ``responded_by_user`` (dict / str / None) -> approvals.json actor."""
    if isinstance(responded_by_user, dict):
        return str(responded_by_user.get("name") or responded_by_user.get("id") or "auto")
    return str(responded_by_user) if responded_by_user else "auto"


def _stage_fn(stage: str):
    """The stage callable for a name in ``stages.order`` (``Blueprint`` validated it)."""
    from swfactory.stages import STAGES

    return STAGES[stage]


# ---------------------------------------------------------------- tasks


def _stage_task(name: str, stage: str, shape: dict[str, Any], outlets: list[Asset]):
    @task(
        task_id=stage,
        retries=STAGE_RETRIES.get(stage, 0),
        execution_timeout=shape["stage_timeout"],
        max_active_tis_per_dagrun=shape["max_parallel_jobs"],
        outlets=outlets,
        # deliver publishes rejected runs too: it must survive the skip cascade a reject starts.
        trigger_rule="none_failed" if stage == "deliver" else "all_success",
    )
    def _run(job: dict, **context: Any) -> dict:
        ctx = _ctx(name, job, context["dag_run"].run_id)
        return _stage_fn(stage)(ctx).model_dump()

    return _run


class GateOperator(ApprovalOperator):
    """``ApprovalOperator`` whose Reject skips nothing by itself.

    The stock operator skips its downstream on Reject — its direct child unconditionally
    (``NotPreviouslySkippedDep`` ignores trigger rules), so ``record_<stage>`` would never see
    the refusal or the approver. Here the response is returned as XCom for both decisions and
    ``record_<stage>`` persists it, then short-circuits the line itself.
    """

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        ret = HITLOperator.execute_complete(self, context=context, event=event)
        self.hitl_summary_extra["approved"] = ret["chosen_options"][0] == self.APPROVE
        return ret


def _approve_task(name: str, stage: str, gate: dict[str, Any]) -> ApprovalOperator:
    auto = bool(gate.get("auto", False)) or APPROVE_ENV_AUTO
    issue = "{{ ti.xcom_pull(task_ids='fan_out')[ti.map_index]['issue'] }}"
    preview = (
        f"{{{{ (ti.xcom_pull(task_ids='{GROUP_ID}.{stage}', map_indexes=ti.map_index) or {{}})"
        ".get('preview', '') }}"
    )
    assigned = [str(u) for u in gate.get("assigned") or []]
    return GateOperator(
        task_id=f"approve_{stage}",
        subject=f"[{name}] approve {gate['artifact']} for {issue}",
        body=f"Run {{{{ dag_run.run_id }}}} · job {{{{ ti.map_index }}}} · `{gate['artifact']}`\n\n"
        + preview,
        defaults=ApprovalOperator.APPROVE if auto else None,
        response_timeout=timedelta(hours=int(gate.get("timeout_h", 24))),
        # HITLUser is {"id", "name"}; an empty `assigned` means anyone may answer.
        assigned_users=[{"id": u, "name": u} for u in assigned] or None,
    )


def _record_task(name: str, stage: str):
    @task(task_id=f"record_{stage}")
    def _run(job: dict, **context: Any) -> dict:
        from datetime import UTC, datetime

        from airflow.sdk.exceptions import AirflowSkipException

        from swfactory.models import Approval
        from swfactory.stages import record_approval

        ti = context["ti"]
        # Marked-success gates (`airflow dags test --mark-success-pattern`) leave no XCom -> auto.
        response = (
            ti.xcom_pull(task_ids=f"{GROUP_ID}.approve_{stage}", map_indexes=ti.map_index) or {}
        )
        chosen = (response.get("chosen_options") or [ApprovalOperator.APPROVE])[0]
        approval = Approval(
            gate=stage,
            decision="approve" if chosen == ApprovalOperator.APPROVE else "reject",
            actor=_actor(response.get("responded_by_user")),
            at=datetime.now(UTC),
        )
        record_approval(_ctx(name, job, context["dag_run"].run_id), approval)
        if approval.decision == "reject":  # work stages skip; deliver/metrics/teardown still run
            raise AirflowSkipException(f"{stage} rejected by {approval.actor}")
        return approval.model_dump(mode="json")

    return _run


def _setup_task(name: str, shape: dict[str, Any]):
    @task(task_id="setup", retries=2, execution_timeout=shape["stage_timeout"])
    def setup(job: dict, **context: Any) -> dict:
        from swfactory import stages

        return stages.setup(_ctx(name, job, context["dag_run"].run_id)).model_dump()

    return setup


def _metrics_task(name: str):
    @task(task_id="metrics", trigger_rule="none_failed")
    def metrics(job: dict, **context: Any) -> dict:
        import json

        from swfactory.config import Config

        ctx = _ctx(name, job, context["dag_run"].run_id)
        path = f"{Config.artifacts_dir(ctx.issue.id)}/metrics.json"
        return json.loads(ctx.read_artifact(path)) if ctx.state.has_artifact(path) else {}

    return metrics


def _teardown_task(name: str):
    @task(task_id="teardown", trigger_rule="all_done")
    def teardown(job: dict, **context: Any) -> None:
        # `_ctx` never calls sb.ensure(), so closing here can only stop what setup() created.
        _ctx(name, job, context["dag_run"].run_id).sb.close()

    return teardown


# ---------------------------------------------------------------- DAG per blueprint


def build_dag(shape: dict[str, Any]) -> DAG:
    """Emit the DAG for one blueprint shape (see module docstring for the task layout)."""
    name = shape["name"]
    metrics_asset = Asset(name=f"swf.metrics.{name}")

    with DAG(
        dag_id=name,
        schedule=shape["cron"],
        catchup=False,
        params={
            "issues": Param([], type="array", description="issue numbers or issue .md paths"),
            "issue": Param("", type="string", description="single issue (compat)"),
        },
        tags=["swfactory"],
        doc_md=__doc__,
    ) as dag:

        @task(task_id="fan_out")
        def fan_out(**context: Any) -> list[dict]:
            from swfactory.blueprint import load

            return load(name).jobs(context["dag_run"].conf or {})

        @task_group(group_id=GROUP_ID)
        def job(job: dict) -> None:
            setup = _setup_task(name, shape)(job)
            prev = setup
            for stage in shape["order"]:
                outlets = [metrics_asset] if stage == "deliver" else []
                current = _stage_task(name, stage, shape, outlets)(job)
                prev >> current
                prev = current
                gate = shape["gates"].get(stage)
                if gate is not None:
                    approve = _approve_task(name, stage, gate)
                    record = _record_task(name, stage)(job)
                    prev >> approve >> record
                    prev = record
            metrics = _metrics_task(name)(job)
            prev >> metrics
            metrics >> _teardown_task(name)(job).as_teardown(setups=setup)

        job.expand(job=fan_out())

    return dag


_only = get_parsing_context().dag_id
for _path in sorted(BLUEPRINTS_DIR.glob("*.toml")):
    _shape = read_shape(_path)
    if _only is None or _only == _shape["name"]:
        globals()[f"dag_{_shape['name']}"] = build_dag(_shape)
