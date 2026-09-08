---
id: COLLAPSE-2034
title: Make the capability inventory the single public truth for every claimed feature
labels: [factory, selfhost]
---
## Problem

`config/capability-inventory.json` is the best honesty mechanism in the repo: eleven claims, each with `state`, `support`, `invariant`, `owner`, `runtime_entry`, `environment`, `test`, `evidence`, and a mandatory `follow_up` on anything experimental. It is structurally validated in the required `test` job.

But it is not the source of the claims a reader actually sees. README and the site describe features in prose, independently. So a feature can read as shipped on the site while the inventory calls it experimental - and until recently `sandbox.islo` had no claim at all despite being the default sandbox and the documented production deployment.

## Acceptance

- Every feature statement in `README.md` and `site/` that corresponds to a claim is **generated from, or explicitly linked to**, its inventory row and that row's `support` value: `supported` / `experimental` / `test_only` / `unsupported`.
- A test fails if a feature is described as available in README or the site without a corresponding claim, or with a stronger support level than the claim carries.
- The inventory's `test` and `evidence` strings are validated as *resolvable*, not merely present: a named test file must exist, and a named CI job must exist in `.github/workflows/`. Today nothing checks either, so a claim can cite a test that was deleted.

## Why this one matters most

This is the mechanism that keeps the rest of the convergence honest. Once the matrix is data and the physics is quarantined, the inventory is the only thing standing between a reader and an overstated capability.

