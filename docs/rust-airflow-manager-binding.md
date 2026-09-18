# Rust manager ↔ Airflow binding

The contract is deliberately asymmetric:

- **Rust owns factory identity and semantics.**
- **Airflow owns lifecycle scheduling.**
- The bridge is public REST v2 plus a small versioned JSON document.

## Harness entry

A harness talks only to the Rust binary:

```bash
swf factory run research \
  --harness codex \
  --factory-id session-123 \
  --issue 42 \
  --target owner/repo \
  --json
```

Rust derives a stable logical `FactoryRunId` from the governed harness request. That identity does
not contain the Airflow run id.

## Rust → Airflow

The manager posts:

```json
{
  "logical_date": null,
  "conf": {
    "issues": ["42"],
    "targets": ["owner/repo"],
    "_swf_manager": {
      "api_version": 1,
      "factory_run_id": "frun_...",
      "harness": "codex",
      "factory_session": "session-123"
    }
  }
}
```

Only `issues` and `targets` are lifecycle inputs consumed by the DAG today.
`_swf_manager` is binding metadata: Airflow may carry it and expose it to callbacks, but it is not
allowed to reinterpret factory authority.

The resulting `dag_id + dag_run_id` is stored/returned as `SchedulerBinding`, never as the logical
run identity.

## Airflow → Rust target

The next migration slice is a tiny Airflow operator that constructs a versioned `StageInvocation`
and calls the Rust manager over HTTP or a Unix-domain socket:

```text
Airflow task
  -> StageInvocation
  -> Rust manager API
  -> swf-app stage use case
  -> StageReceipt
  -> Airflow maps disposition to success / defer / retry / fail
```

No admission policy, retry-safety rule, publication credential, evidence fan-in, sandbox semantics,
or Cell-epoch decision belongs in Python.
