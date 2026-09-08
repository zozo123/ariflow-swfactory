# Spec — SELFHOST-1: hermetic coverage for the blueprint files

## Requirements
1. A new test module `tests/test_blueprint_files.py` discovers `blueprints/*.toml` from the repo
   root and asserts the directory is non-empty.
2. Every discovered file loads through `swfactory.blueprint.load` without raising.
3. `[blueprint].name` values are unique across the directory, because the name becomes the
   Airflow `dag_id` and a collision silently drops a line from the DagBag.
4. Every blueprint declares at least one `[[targets]]` entry, and its stage order starts at
   `intent` and ends at `deliver`.
5. Every blueprint's `[sandbox] ttl_s` is strictly greater than its longest gate timeout in
   seconds, so a work cell cannot expire while a human gate is still open.

## API
No production API change. Tests only.

## Concerns
The module must stay hermetic: it reads files from the repo and calls the existing pydantic
loader, with no network, no subprocess and no Airflow import.

## Open questions
None.
