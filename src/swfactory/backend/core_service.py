"""Backend projection of the canonical mutation/recovery truth.

The module owns no state and schedules nothing. It joins the Factory's existing Cell store,
operation journal and evidence ledger so SCM mutations and operator inspection see the same truth.
"""

from __future__ import annotations

from typing import Any

from swfactory.operation_recovery import plan_recovery

from .service import Factory, Refused, has_cleanup_debt


def operator_projection(factory: Factory, cell_id: str) -> dict[str, Any]:
    try:
        truth = factory.control.core.inspect(cell_id)
    except KeyError as error:
        # `CoreCapabilityRuntime.inspect` reads the Cell store directly rather than going through
        # `Factory._cell`, so an unknown id escaped as a bare KeyError, reached the transport's
        # catch-all and told an operator who mistyped a cell id that the backend was broken. Every
        # sibling cell route answers 404 naming the id; this one now matches them.
        raise Refused(404, f"no such Factory Cell {cell_id}") from error
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
    cleanup_debt = has_cleanup_debt(cell)
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


def recovery_plan(factory: Factory, operation_key: str) -> dict[str, Any]:
    try:
        row = factory.control.operations.get(operation_key)
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
