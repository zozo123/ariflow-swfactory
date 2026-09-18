# Rust manager ↔ Airflow binding

Airflow remains the lifecycle scheduler. The Rust manager owns factory semantics.

The binding is deliberately small:

1. Rust admits a logical factory run.
2. Rust triggers Airflow through REST API v2 and records the scheduler binding.
3. An Airflow task invokes one Rust manager stage with a versioned `StageInvocation`.
4. Rust validates FactoryRun/Cell/epoch/attempt identity, executes the use case and returns a `StageReceipt`.
5. Airflow maps the receipt to success, deferral/retry or failure. It does not reinterpret the factory state.

Preferred transport is authenticated HTTP for multi-host deployments and a Unix-domain socket for a
single-host deployment. The payload is identical on both.

Python Airflow code should eventually be boring: construct the invocation from task context, call the
manager, map the returned disposition to Airflow lifecycle behavior.

It must not contain:
- admission policy;
- stage implementation;
- publication credentials;
- retry safety logic;
- Cell epoch decisions;
- evidence fan-in;
- provider-specific sandbox semantics.

Those remain Rust application/runtime responsibilities.
