"""One Airflow DAG per ``blueprints/*.toml``: the software-factory line as a mapped task group.

For every blueprint file this module emits ``DAG(dag_id=<blueprint.name>)``::

    fan_out -> job[ setup -> <stage> (-> approve_<stage> -> record_<stage>)* ... -> deliver
                    -> metrics ; teardown ]            (job = one (issue x target), mapped)

``fan_out`` turns ``dag_run.conf`` into jobs; a scheduled line falls back to its required trigger
issues. The ``job`` task group is expanded over them, so one issue can be applied to N target repos
with one addressable approval per (issue,target). Backend-managed submissions additionally carry
verified Factory Cell id/epoch/policy bindings. A cron-created run has no submitter to carry them,
so ``fan_out`` admits it through the backend's work orders itself and stops if it cannot. Direct
legacy Airflow submissions derive the same cell id but remain explicitly unmanaged. Airflow is the
only lifecycle scheduler; managed tasks report state back to the backend solely for epoch-fenced
authority, admission and evidence.

Every DAG declares its run bounds from the blueprint (``Blueprint.schedule_limits``): a cron line
has a timezone-aware origin and one run in flight, and no run outlives the sandbox it runs in.

Loops and bounded ``Plan.work`` execution live inside stage functions, never in the DAG.
"""

from __future__ import annotations

import tomllib
from datetime import datetime, timedelta
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


def gate_mode(gate: dict[str, Any]) -> str:
    """The gate's declared authority: ``"human"`` unless the blueprint says ``mode = "auto"``.

    Duplicated from ``swfactory.approval_policy.declared_mode`` because DAG parsing must not import
    swfactory; ``tests/test_dag_parity.py`` pins the two to the same answer. No environment variable
    is read here on purpose -- ``SWF_APPROVE=auto`` used to be OR'd in, which turned every gate a
    blueprint declared human into a self-approving one.
    """
    mode, auto = gate.get("mode"), gate.get("auto")
    if auto is not None and not isinstance(auto, bool):
        raise ValueError(f"gate auto must be a boolean, not {auto!r}")
    if mode is None:
        return "auto" if auto else "human"
    if mode not in ("human", "auto"):
        raise ValueError(f"gate mode must be one of ['human', 'auto'], not {mode!r}")
    if auto is not None and auto is not (mode == "auto"):
        raise ValueError(f"gate declares mode {mode!r} and auto {auto!r}, which contradict each other")
    return mode


def read_shape(path: Path) -> dict[str, Any]:
    """The subset of a blueprint the DAG structure depends on. Validation happens in tasks, except
    for the schedule origin: a cron DAG without a timezone-aware ``trigger.start`` would be scheduled
    and then fail in ``fan_out`` every tick, so that one is refused at parse time.

    The run bounds duplicate ``swfactory.blueprint.Blueprint.schedule_limits`` because DAG parsing
    must not import swfactory; ``tests/test_dag_parity.py`` pins the two to the same answer.
    """
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    trigger = data.get("trigger", {})
    limits = data.get("limits", {})
    cron = trigger.get("cron") if trigger.get("kind") == "cron" else None
    start = trigger.get("start")
    if cron is not None and start is None:
        raise ValueError(f"{path.name}: trigger.kind='cron' requires trigger.start")
    if start is not None and (not isinstance(start, datetime) or start.tzinfo is None):
        raise ValueError(f"{path.name}: trigger.start must be a timezone-aware datetime")
    return {
        "name": data.get("blueprint", {}).get("name") or path.stem,
        "cron": cron,
        "start": start,
        "max_active_runs": int(trigger.get("max_active_runs") or (1 if cron else 16)),
        # a run may live no longer than its sandbox: past the TTL the cell is gone either way
        "run_timeout": timedelta(seconds=int(data.get("sandbox", {}).get("ttl_s", 172_800))),
        "order": list(data["stages"]["order"]),
        "gates": {g["after"]: g for g in data.get("gates", [])},
        "stage_timeout": timedelta(hours=int(limits.get("stage_timeout_h", 3))),
        "max_parallel_jobs": int(limits.get("max_parallel_jobs", 4)),
    }


def run_id_for(dag_run_id: str, job_idx: int) -> str:
    from swfactory.runtime import run_id_for as _impl

    return _impl(dag_run_id, job_idx)


def _ctx(name: str, job: dict[str, Any], dag_run_id: str, *, enforce_inputs: bool = True):
    """``build_ctx`` re-reads the blueprint, the issue and this worker's environment every task, so
    it is also where the epoch's accepted inputs are admitted and re-checked (see
    ``swfactory.accepted_inputs``). ``enforce_inputs=False`` is for teardown only: cleanup must
    still close the sandbox of an epoch whose inputs drifted, or the refusal leaks a live cell."""
    from swfactory.blueprint import load
    from swfactory.runtime import build_ctx

    return build_ctx(load(name), job, run_id=run_id_for(dag_run_id, int(job["job_idx"])), enforce_inputs=enforce_inputs)


def _stage_fn(stage: str):
    from swfactory.stage_registry import resolve

    return resolve(stage)


def _cell_transition(job: dict[str, Any], state: str, context: dict[str, Any], suffix: str) -> None:
    """Runtime-only callback; unmanaged/direct Airflow runs are intentionally no-ops."""
    from swfactory.cell_callback import report

    report(job, state, context, suffix)


def _failure_callback(context: dict[str, Any]) -> None:
    """Mark the current managed cell failed without changing Airflow's failure semantics.

    Used to swallow every exception, which is how a backend outage at failure time left the Cell
    ``running`` and its admission unit held forever (#2071); the report is now recorded as debt.
    """
    from swfactory.cell_callback import report_failure

    report_failure(context)


def _stage_task(name: str, stage: str, shape: dict[str, Any], outlets: list[Asset]):
    @task(
        task_id=stage,
        retries=STAGE_RETRIES.get(stage, 0),
        execution_timeout=shape["stage_timeout"],
        max_active_tis_per_dagrun=shape["max_parallel_jobs"],
        outlets=outlets,
        trigger_rule="none_failed" if stage == "deliver" else "all_success",
        on_failure_callback=_failure_callback,
    )
    def _run(job: dict, **context: Any) -> dict:
        ctx = _ctx(name, job, context["dag_run"].run_id)
        result = _stage_fn(stage)(ctx).model_dump()
        if stage == "deliver":
            _cell_transition(job, "success", context, "delivered")
        return result

    return _run


class GateOperator(ApprovalOperator):
    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        ret = HITLOperator.execute_complete(self, context=context, event=event)
        self.hitl_summary_extra["approved"] = ret["chosen_options"][0] == self.APPROVE
        return ret


def _approve_task(name: str, stage: str, gate: dict[str, Any], mode: str) -> ApprovalOperator:
    issue = "{{ ti.xcom_pull(task_ids='fan_out')[ti.map_index]['issue'] }}"
    preview = (
        f"{{{{ (ti.xcom_pull(task_ids='{GROUP_ID}.{stage}', map_indexes=ti.map_index) or {{}}).get('preview', '') }}}}"
    )
    assigned = [str(u) for u in gate.get("assigned") or []]
    return GateOperator(
        task_id=f"approve_{stage}",
        subject=f"[{name}] approve {gate['artifact']} for {issue}",
        body=f"Run {{{{ dag_run.run_id }}}} · job {{{{ ti.map_index }}}} · `{gate['artifact']}`\n\n" + preview,
        defaults=ApprovalOperator.APPROVE if mode == "auto" else None,
        response_timeout=timedelta(hours=int(gate.get("timeout_h", 24))),
        assigned_users=[{"id": u, "name": u} for u in assigned] or None,
    )


def _record_task(name: str, stage: str, mode: str):
    @task(task_id=f"record_{stage}", on_failure_callback=_failure_callback)
    def _run(job: dict, **context: Any) -> dict:
        from airflow.sdk.exceptions import AirflowSkipException

        from swfactory.approval_policy import approval_from_response
        from swfactory.stages import record_approval

        ti = context["ti"]
        response = ti.xcom_pull(task_ids=f"{GROUP_ID}.approve_{stage}", map_indexes=ti.map_index)
        ctx = _ctx(name, job, context["dag_run"].run_id)
        # A missing/empty/malformed response raises here rather than defaulting to Approve: the
        # gate task can be marked successful without anyone answering, and that is not an approval.
        approval = approval_from_response(
            gate=stage,
            gate_mode=mode,
            response=response,
            fixture_path=ctx.cfg.gate_replay,
            managed=bool(job.get("cell_managed")),
            scm=ctx.cfg.scm,
            agent=ctx.cfg.agent,
        )
        record_approval(ctx, approval)
        if approval.decision == "reject":
            _cell_transition(job, "rejected", context, f"rejected_{stage}")
            raise AirflowSkipException(f"{stage} rejected by {approval.actor}")
        return approval.model_dump(mode="json")

    return _run


def _setup_task(name: str, shape: dict[str, Any]):
    @task(
        task_id="setup",
        retries=2,
        execution_timeout=shape["stage_timeout"],
        # setup is where a cell comes to exist; unbounded, N setups meant N live cells regardless
        # of the parallelism the stages after it declare
        max_active_tis_per_dagrun=shape["max_parallel_jobs"],
        on_failure_callback=_failure_callback,
    )
    def setup(job: dict, **context: Any) -> dict:
        from swfactory import stages

        result = stages.setup(_ctx(name, job, context["dag_run"].run_id)).model_dump()
        _cell_transition(job, "running", context, "setup")
        return result

    return setup


def _metrics_task(name: str):
    @task(task_id="metrics", trigger_rule="none_failed", on_failure_callback=_failure_callback)
    def metrics(job: dict, **context: Any) -> dict:
        import json

        from swfactory.config import Config

        ctx = _ctx(name, job, context["dag_run"].run_id)
        path = f"{Config.artifacts_dir(ctx.issue.id)}/metrics.json"
        return json.loads(ctx.read_artifact(path)) if ctx.state.has_artifact(path) else {}

    return metrics


def _teardown_task(name: str):
    @task(task_id="teardown", trigger_rule="all_done", retries=2)
    def teardown(job: dict, **context: Any) -> None:
        from swfactory import stages

        stages.teardown(_ctx(name, job, context["dag_run"].run_id, enforce_inputs=False))
        _cell_transition(job, "cleaned", context, "teardown")

    return teardown


def build_dag(shape: dict[str, Any]) -> DAG:
    name = shape["name"]
    metrics_asset = Asset(name=f"swf.metrics.{name}")

    with DAG(
        dag_id=name,
        schedule=shape["cron"],
        start_date=shape["start"],
        catchup=False,
        max_active_runs=shape["max_active_runs"],
        dagrun_timeout=shape["run_timeout"],
        params={
            "issues": Param([], type="array", description="issue numbers or issue .md paths"),
            "issue": Param("", type="string", description="single issue (compat)"),
        },
        tags=["swfactory"],
        doc_md=__doc__,
    ) as dag:

        @task(task_id="fan_out")
        def fan_out(**context: Any) -> list[dict]:
            from swfactory import cell_runtime
            from swfactory.blueprint import load

            dag_run = context["dag_run"]
            conf = dag_run.conf or {}
            jobs = load(name).jobs(conf)
            bindings = conf.get("_factory_cells")
            if bindings is not None and not isinstance(bindings, list):
                raise ValueError("_factory_cells must be an array")
            if dag_run.run_type == "scheduled":
                # A cron tick admits nothing by itself. The run obtains its Cell bindings from the
                # backend here, before any sandbox exists, or raises and the line stops -- there is
                # no unmanaged fallback for a governed line.
                bindings = cell_runtime.admit_scheduled_run(name, dag_run.run_id, jobs)
            return cell_runtime.bind_jobs(jobs, bindings)

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
                    mode = gate_mode(gate)
                    approve = _approve_task(name, stage, gate, mode)
                    record = _record_task(name, stage, mode)(job)
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
