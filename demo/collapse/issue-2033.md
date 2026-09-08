---
id: COLLAPSE-2033
title: Collapse the legacy backlog tranches into spec coverage rows
labels: [factory, selfhost]
---
## Problem

`src/swfactory/legacy_bundle_01..04.py` each declare a `LegacyTranche(id, lo, hi)`, a tuple of ten `LegacyArea` members, and an `execute_matrix()` that runs area x concern. `legacy_issue_runtime.py` backs them. Nothing outside the family imports the four bundles; `legacy_issue_runtime.py` is imported only by them and by `liquid_release.py`.

Declared coverage of a legacy backlog is useful information. Four Python modules asserting it are not — and `docs/liquid-methodology.md` is explicit that declared coverage must be tracked separately from integrated, validated behaviour.

## Acceptance

- The tranche ranges and their `LegacyArea` coverage become rows in `config/liquid-spec.yaml`, with `state` marking them as declared scope rather than validated behaviour.
- Verify and report whether the four tranches tile their rank range contiguously with no gap or double-count. A discovered gap is a finding worth keeping in the issue trail rather than quietly normalising.
- `legacy_bundle_*.py` and `legacy_issue_runtime.py` are deleted.
- The "Matrix and legacy scope" row in `docs/liquid-methodology.md`'s implementation map currently links `liquid_bundle_engine.py`, `liquid_release.py` and `legacy_issue_runtime.py`. Those links must be repointed at the spec, not left dangling.
