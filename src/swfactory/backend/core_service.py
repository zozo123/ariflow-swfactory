"""Backend projection of the canonical mutation/recovery truth.

The module owns no state and schedules nothing. It joins the Factory's existing Cell store,
operation journal and evidence ledger so SCM mutations and operator inspection see the same truth.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from swfactory.core_capabilities import AirflowBinding, CoreCapabilityRuntime
from swfactory.operation_recovery import plan_recovery

from .service import Factory, Refused, text


def ensure_core(factory: Factory) -> CoreCapabilityRuntime:
    """Bind the existing Factory stores to the canonical facade exactly once."""
    if factory.control.core is None:
        factory.control.core = CoreCapabilityRuntime(
            cells=factory.cell_store,
            journal=factory.control.operations,
            evidence=factory.evidence,
        )
    return factory.control.core


def airflow_binding(factory: Factory, cell_id: str, epoch: int) -> AirflowBinding:
    cell = factory._cell(cell_id)
    if int(cell["epoch"]) != epoch:
        raise Refused(409, f"stale Factory Cell epoch {epoch}; current epoch is {cell['epoch']}")
    dag_id = str(cell.get("airflow_dag_id") or "")
    run_id = str(cell.get("airflow_run_id") or "")
    if not dag_id or not run_id:
        raise Refused(409, "Factory Cell is not bound to an authoritative Airflow run")
    map_index = cell.get("map_index")
    return AirflowBinding(
        cell_id=cell_id,
        epoch=epoch,
        dag_id=dag_id,
        run_id=run_id,
        map_index=int(map_index) if map_index is not None else None,
    )


def intent_digest(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def operator_projection(factory: Factory, cell_id: str) -> dict[str, Any]:
    core = ensure_core(factory)
    truth = core.inspect(cell_id)
    cell = truth["cell"]
    current_epoch = int(cell["epoch"])
    recovery = []
    for row in truth["unresolved_operations"]:
        decision = plan_recovery(row, current_epoch=current_epoch)
        recovery.append(
            {
                "operation_key": row["operation_key"],
                "state": row["state"],
                "observation": row.get("observation"),
                "last_error": row.get("last_error"),
                "next_action": decision.to_dict(),
            }
        )
    cleanup_debt = bool(
        cell.get("compute")
        and not cell.get("cleanup")
        and cell.get("state") in {"success", "failed", "cancelled", "rejected"}
    )
    return {
        "schema_version": 1,
        **truth,
        "recovery": recovery,
        "cleanup_debt": cleanup_debt,
        "operator_truth": {
            "cell_id": cell_id,
            "epoch": current_epoch,
            "owner": cell.get("owner"),
            "state": cell.get("state"),
            "airflow": {
                "dag_id": cell.get("airflow_dag_id"),
                "run_id": cell.get("airflow_run_id"),
                "map_index": cell.get("map_index"),
            },
            "evidence_verified": truth["evidence"]["verified"],
            "evidence_tail": truth["evidence"]["tail_digest"],
            "unresolved_operations": len(truth["unresolved_operations"]),
            "cleanup_debt": cleanup_debt,
        },
    }


def operation(factory: Factory, path: str, body: dict[str, Any]) -> Any:
    if path == "/core/inspect":
        return operator_projection(factory, text(body, "cell_id"))
    if path == "/core/recovery-plan":
        key = text(body, "operation_key")
        try:
            row = factory.control.operations.get(key)
        except KeyError as error:
            raise Refused(404, "no such operation") from error
        cell = factory._cell(str(row["cell_id"]))
        decision = plan_recovery(row, current_epoch=int(cell["epoch"]))
        return {
            "operation": row,
            "cell": {
                "cell_id": cell["cell_id"],
                "epoch": cell["epoch"],
                "state": cell["state"],
                "owner": cell.get("owner"),
            },
            "decision": decision.to_dict(),
        }
    raise Refused(404, "unknown canonical core operation")
