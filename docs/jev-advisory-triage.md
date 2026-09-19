# Jev advisory triage shadow mode

Jev is an optional adviser over a deterministic related-work shortlist. It is not a deduplication
authority and it is not part of execution or promotion.

The first operator flow is:

```text
new proposal
  -> deterministic related-issue shortlist
  -> optional Jev relationship hints
  -> acceptance criteria shown side by side
  -> operator decides
  -> existing Airflow lifecycle executes chosen work
```

A relationship such as `potential_duplicate` means "compare these before filing another issue."
It cannot close, suppress, enroll, execute, approve, merge, or promote anything.

If either side lacks observable acceptance criteria, the view forces `needs_review=true` even when
the adviser reports high confidence. The permanent regression case is "Fix the flaky upload" versus
"Make upload reliable": confidence is not evidence that the two tasks share the same done-condition.

Provider failure, timeout, malformed output, or missing credentials degrades to an unavailable
advisory and preserves the proposal. Shadow evaluation reports mistakes, review/abstention and false
duplicate suggestions; it does not make product-quality or latency claims without held-out evidence.
