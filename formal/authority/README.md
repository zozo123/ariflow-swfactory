# Authority-kernel formal model

This directory contains an **experimental design model**, not a proof that the Python/Rust implementation is correct.

`AuthorityKernel.tla` models one Factory Cell across epoch replacement, Airflow binding, candidate freeze/evidence/approval, ambiguous external effects, trusted authority grants, and publication. The environment may present stale effect requests, arbitrary search-phase changes, and authority requests from either trusted or search sources. The model refuses stale effects, rejects search-origin authority, and blocks epoch activation while an external effect is pending or `in_doubt`.

Initial invariants:

- evidence refers only to the frozen candidate;
- approval requires evidence for that same candidate;
- publication requires frozen candidate = evidence = approval under the current Airflow-bound epoch;
- an external commit belongs to the current epoch;
- search-origin authority requests are modeled explicitly but cannot become an authority source;
- an `in_doubt` effect has no blind retry or epoch-activation escape and leaves that state only through observation.

Run with a local TLA+/TLC installation:

```sh
cd formal/authority
tlc AuthorityKernel.tla -config AuthorityKernel.cfg
```

TLC is intentionally **not** a required CI dependency in this first slice. The model is therefore a formal artifact whose scope and assumptions are explicit, not yet a runtime guarantee.

The next step is refinement evidence: project real `ControlKernel` / `CoreCapabilityRuntime` events into this action vocabulary and check that retained traces are admitted by the model. Until that implementation-to-model bridge exists, the repository must say "the model satisfies these invariants under these assumptions", never "TLA+ proved the factory safe".
