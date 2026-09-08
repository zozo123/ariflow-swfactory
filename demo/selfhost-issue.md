---
id: SELFHOST-1
title: Guard the blueprint files the Airflow DAGs are built from
labels: [factory, selfhost]
---
`dags/blueprints.py` builds one Airflow DAG per file in `blueprints/`, and `[blueprint].name` is
the `dag_id`. Nothing in the hermetic suite checks those files directly, so a malformed TOML or a
duplicated name is only discovered when the scheduler parses the DAG bag - which takes down every
line at once, including the line that would ship the fix.

Add hermetic coverage over `blueprints/*.toml`:

Acceptance:
- Every file loads through `swfactory.blueprint.load`.
- Names are unique, because the name is the `dag_id`.
- Each declares at least one target, starts at `intent` and ends at `deliver`.
- Each sandbox `ttl_s` outlives its longest gate, so a cell cannot expire while a human gate is
  still open.
- Tests only: do not change any blueprint, and do not touch the factory's own control plane.
