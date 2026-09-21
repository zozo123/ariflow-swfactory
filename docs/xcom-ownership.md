# XCom ownership

Airflow is the lifecycle scheduler. **XCom is scratch space, not authority.**

| Datum | XCom | Source of truth |
| --- | --- | --- |
| issue/target routing, Cell id + epoch | allowed | backend work order + CellStore |
| bounded stage preview / stage result | allowed, untrusted | run artifact store |
| HITL approve/reject response | allowed as an untrusted response | sealed `approvals.json` + Cell/inputs/artifact checks |
| policy digest / generation seal | **never** | CellStore |
| credential lease / handle / redeem material | **never** | credential broker |
| raw patch bytes | **never** | sandbox/worktree then BackendScm request |
| GitHub/backend bearer tokens | **never** | trusted backend/gateway |

`swfactory.xcom_contract.validate_xcom_document` enforces the forbidden-field portion of this
matrix. Managed mapped jobs intentionally carry only Cell identity/epoch; every task re-reads the
current policy authority from the backend before creating its runtime context.

A HITL response pulled from XCom is treated as input, not as a seal. `record_approval` binds the
answer to the current Cell epoch, accepted-input digest and exact artifact digest, and `deliver`
re-validates that sealed record before any external effect. Corrupting scheduler scratch therefore
cannot substitute a policy digest or credential capability.
