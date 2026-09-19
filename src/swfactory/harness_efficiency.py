"""Local harness-efficiency primitives that preserve exact evidence.

The model never decides whether an observation may disappear. Large diagnostic output is archived
verbatim in host-owned run state and mirrored into ignored sandbox scratch for exact recall. A
compact repair prompt may quote only verified line ranges from that archive; if the reducer cannot
prove its quotations or cannot make the prompt smaller, it falls back to a bounded legacy tail.

This is deliberately an execution-layer optimization. It has no Airflow, approval, publication, or
promotion dependency and therefore cannot mint authority from a cache or a compacted observation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from swfactory.stages import Ctx

_STDOUT_TAIL = 6000
_STDERR_TAIL = 2000
_PROMPT_BUDGET_BYTES = 5600
_CONTEXT_LINES = 2
_TAIL_LINES = 24
_HEAD_LINES = 5
_ERROR_RE = re.compile(
    r"(?i)(traceback|assertionerror|\bfailed\b|\bfailure\b|\berror\b|\bpanic\b|"
    r"\bexception\b|caused by|##\[error\]|^E\s+|^error:)"
)
_GENERIC_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(api[_-]?key|access[_-]?token|auth[_-]?token|secret|password|passwd|private[_-]?key)"
    r"\s*[:=]\s*[^\s]{8,}"
)


class ObservationIntegrityError(ValueError):
    """An observation or reducer receipt no longer matches its archived source."""


@dataclass(frozen=True)
class ObservationRef:
    handle: str
    sha256: str
    command_sha256: str
    exit_code: int
    timed_out: bool
    state_path: str
    sandbox_path: str | None
    sensitivity_kinds: tuple[str, ...]
    size_bytes: int
    lines: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Quote:
    start_line: int
    end_line: int
    text: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()

    def public_dict(self) -> dict[str, object]:
        return {
            "start_line": self.start_line,
            "end_line": self.end_line,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class PackedObservation:
    ref: ObservationRef
    prompt_text: str
    mode: Literal["reduced", "legacy-with-handle"]
    legacy_bytes: int
    prompt_bytes: int
    quotes: tuple[Quote, ...]

    @property
    def saved_bytes(self) -> int:
        return max(self.legacy_bytes - self.prompt_bytes, 0)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source(*, command: str, exit_code: int, timed_out: bool, stdout: str, stderr: str) -> str:
    """One exact, unambiguous source for the execution metadata and both process streams."""

    return (
        "=== execution ===\n"
        f"command: {json.dumps(command, ensure_ascii=True)}\n"
        f"exit_code: {exit_code}\n"
        f"timed_out: {str(timed_out).lower()}\n"
        f"=== stdout ===\n{stdout}\n=== stderr ===\n{stderr}"
    )


def _legacy_tail(stdout: str, stderr: str) -> str:
    """The pre-ObservationPack repair context, retained as the fallback contract."""

    return (stdout[-_STDOUT_TAIL:] + "\n" + stderr[-_STDERR_TAIL:]).strip()


def _sensitivity_kinds(text: str) -> tuple[str, ...]:
    """Conservative local classifier for whether raw diagnostics may be agent-readable."""

    from swfactory.scm import scan_secrets

    kinds = set(scan_secrets(text.encode("utf-8")))
    if _GENERIC_SECRET_ASSIGNMENT.search(text):
        kinds.add("generic-secret-assignment")
    return tuple(sorted(kinds))


def _archive(
    ctx: Ctx,
    source: str,
    *,
    command: str,
    exit_code: int,
    timed_out: bool,
) -> ObservationRef:
    digest = _sha256(source)
    state_path = f"harness/observations/{digest}.txt"
    sandbox_path = f".factory/observations/{digest}.txt"

    try:
        existing = ctx.state.read_artifact(state_path)
    except FileNotFoundError:
        ctx.state.write_artifact(state_path, source)
    else:
        if _sha256(existing) != digest or existing != source:
            raise ObservationIntegrityError(f"observation archive collision for sha256:{digest}")

    sensitivity = _sensitivity_kinds(source)
    agent_path: str | None = None
    if not sensitivity:
        ctx.sb.write(sandbox_path, source)
        agent_path = sandbox_path

    lines = len(source.splitlines())
    return ObservationRef(
        handle=f"obs:sha256:{digest}",
        sha256=digest,
        command_sha256=_sha256(command),
        exit_code=exit_code,
        timed_out=timed_out,
        state_path=state_path,
        sandbox_path=agent_path,
        sensitivity_kinds=sensitivity,
        size_bytes=len(source.encode("utf-8")),
        lines=lines,
    )


def exact_page(source: str, *, start_line: int = 1, limit: int = 200) -> str:
    """Return an exact 1-based line page for deterministic recall."""

    if start_line < 1 or limit < 1:
        raise ValueError("start_line and limit must be positive")
    lines = source.splitlines()
    return "\n".join(lines[start_line - 1 : start_line - 1 + limit])


def verify_quotes(source: str, quotes: tuple[Quote, ...]) -> None:
    """Fail unless every quotation is exactly the archived source slice it claims."""

    lines = source.splitlines()
    for quote in quotes:
        if quote.start_line < 1 or quote.end_line < quote.start_line or quote.end_line > len(lines):
            raise ObservationIntegrityError(
                f"quote range {quote.start_line}-{quote.end_line} is outside a {len(lines)} line observation"
            )
        exact = "\n".join(lines[quote.start_line - 1 : quote.end_line])
        if exact != quote.text:
            raise ObservationIntegrityError(
                f"quote range {quote.start_line}-{quote.end_line} does not match archived source"
            )


def _merge(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge zero-based half-open line ranges."""

    if not ranges:
        return []
    merged: list[list[int]] = []
    for start, end in sorted(ranges):
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _quote_ranges(source: str) -> tuple[Quote, ...]:
    lines = source.splitlines()
    if not lines:
        return ()

    error_ranges: list[tuple[int, int]] = []
    for index, line in enumerate(lines):
        if _ERROR_RE.search(line):
            error_ranges.append((max(0, index - _CONTEXT_LINES), min(len(lines), index + _CONTEXT_LINES + 1)))

    candidates = list(reversed(_merge(error_ranges)))
    candidates += [
        (max(0, len(lines) - _TAIL_LINES), len(lines)),
        (0, min(_HEAD_LINES, len(lines))),
    ]

    selected: list[tuple[int, int]] = []
    used = 0
    for start, end in candidates:
        if start >= end:
            continue
        text = "\n".join(lines[start:end])
        cost = len(text.encode("utf-8")) + 48
        if cost > _PROMPT_BUDGET_BYTES:
            continue
        if used + cost > _PROMPT_BUDGET_BYTES and selected:
            continue
        selected.append((start, end))
        used += cost

    quote_rows: list[Quote] = []
    for start, end in _merge(selected):
        quote_rows.append(
            Quote(
                start_line=start + 1,
                end_line=end,
                text="\n".join(lines[start:end]),
            )
        )
    quotes = tuple(quote_rows)
    verify_quotes(source, quotes)
    return quotes


def _reduced_prompt(ref: ObservationRef, quotes: tuple[Quote, ...]) -> str:
    recall = (
        f"exact source: {ref.sandbox_path}\n"
        "Use Read on the exact source with an offset/limit when an omitted line matters. "
        if ref.sandbox_path
        else "full source: host-only because the local secret classifier matched; raw recall is disabled. "
    )
    header = (
        "TEST OUTPUT COMPACTED LOCALLY; THE HOST ARCHIVE IS AUTHORITATIVE.\n"
        f"handle: {ref.handle}\n"
        f"{recall}\n"
        f"sha256: {ref.sha256}\n"
        f"source: {ref.size_bytes} bytes, {ref.lines} lines\n"
        "Every excerpt below was verified byte-for-byte against that source before exposure.\n"
    )
    blocks: list[str] = []
    for quote in quotes:
        blocks.append(f"\n[exact lines {quote.start_line}-{quote.end_line}]\n{quote.text}")
    return (header + "".join(blocks)).strip()


def pack_failure_observation(
    ctx: Ctx,
    *,
    command: str,
    exit_code: int,
    timed_out: bool,
    stdout: str,
    stderr: str,
) -> PackedObservation | None:
    """Archive a truncated failure and return a smaller, exactly-recallable repair observation.

    Small failures retain the legacy behavior exactly and return None. The optimization engages
    only when the old code would have discarded output.
    """

    if len(stdout) <= _STDOUT_TAIL and len(stderr) <= _STDERR_TAIL:
        return None

    source = _source(
        command=command,
        exit_code=exit_code,
        timed_out=timed_out,
        stdout=stdout,
        stderr=stderr,
    )
    ref = _archive(
        ctx,
        source,
        command=command,
        exit_code=exit_code,
        timed_out=timed_out,
    )
    legacy = _legacy_tail(stdout, stderr)
    verified_quotes = _quote_ranges(source)
    quotes = tuple(quote for quote in verified_quotes if not _sensitivity_kinds(quote.text))
    reduced = _reduced_prompt(ref, quotes)

    if ref.sensitivity_kinds:
        # Never widen exposure beyond the legacy tail when any secret-shaped material exists.
        # The exact source remains host-owned; only independently safe verified excerpts cross
        # the agent boundary.
        prompt = reduced
        mode: Literal["reduced", "legacy-with-handle"] = "reduced"
    elif len(reduced.encode("utf-8")) >= len(legacy.encode("utf-8")):
        prompt = (
            f"FULL TEST OUTPUT ARCHIVED AS {ref.handle} AT {ref.sandbox_path} "
            f"(sha256:{ref.sha256}). Use Read for omitted context.\n\n{legacy}"
        ).strip()
        mode = "legacy-with-handle"
    else:
        prompt = reduced
        mode = "reduced"

    packed = PackedObservation(
        ref=ref,
        prompt_text=prompt,
        mode=mode,
        legacy_bytes=len(legacy.encode("utf-8")),
        prompt_bytes=len(prompt.encode("utf-8")),
        quotes=quotes,
    )

    internal = {
        "schema_version": 1,
        "ref": ref.to_dict(),
        "mode": packed.mode,
        "legacy_bytes": packed.legacy_bytes,
        "prompt_bytes": packed.prompt_bytes,
        "saved_bytes": packed.saved_bytes,
        "quotes": [asdict(quote) | {"sha256": quote.sha256} for quote in quotes],
    }
    ctx.state.write_artifact(
        f"harness/observations/{ref.sha256}.receipt.json",
        json.dumps(internal, indent=2, sort_keys=True) + "\n",
    )

    public = {
        "schema_version": 1,
        "handle": ref.handle,
        "sha256": ref.sha256,
        "command_sha256": ref.command_sha256,
        "exit_code": ref.exit_code,
        "timed_out": ref.timed_out,
        "sensitive": bool(ref.sensitivity_kinds),
        "sensitivity_kinds": list(ref.sensitivity_kinds),
        "size_bytes": ref.size_bytes,
        "lines": ref.lines,
        "mode": packed.mode,
        "legacy_bytes": packed.legacy_bytes,
        "prompt_bytes": packed.prompt_bytes,
        "saved_bytes": packed.saved_bytes,
        "quotes": [quote.public_dict() for quote in quotes],
        "raw_log_committed": False,
        "remote_model_used": False,
    }
    ctx.write_artifact(
        f"{ctx.art}/harness-observations/{ref.sha256}.json",
        json.dumps(public, indent=2, sort_keys=True) + "\n",
    )
    return packed


# ---------------------------------------------------------------- research-loop selection


@dataclass(frozen=True)
class HarnessTrial:
    """One verifier-driven harness experiment.

    authority_digest is a canonical projection of promotion-relevant facts, not a cache key.
    candidate_sha is explicit because an accelerator that changes the accepted bytes changed the
    answer. evidence_digest may differ: an experimental mechanism is allowed to retain additional
    diagnostic evidence as long as that evidence is complete and independently verifiable.
    """

    mechanism: str
    candidate_sha: str
    authority_digest: str
    evidence_digest: str
    prompt_bytes: int
    cost_microusd: int
    wall_ms: int
    model_turns: int
    evidence_complete: bool = True
    verifier_passed: bool = True

    def validate(self) -> None:
        if not self.mechanism.strip():
            raise ValueError("harness mechanism name is required")
        if not re.fullmatch(r"[0-9a-f]{40}", self.candidate_sha):
            raise ValueError("candidate_sha must be a full lowercase git SHA")
        for label, digest in (
            ("authority_digest", self.authority_digest),
            ("evidence_digest", self.evidence_digest),
        ):
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError(f"{label} must be a canonical sha256 digest")
        for label, value in (
            ("prompt_bytes", self.prompt_bytes),
            ("cost_microusd", self.cost_microusd),
            ("wall_ms", self.wall_ms),
            ("model_turns", self.model_turns),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative integer")

    @property
    def efficiency_vector(self) -> tuple[int, int, int, int]:
        return self.prompt_bytes, self.cost_microusd, self.wall_ms, self.model_turns


@dataclass(frozen=True)
class HarnessSelection:
    baseline: str
    survivors: tuple[str, ...]
    refusals: tuple[str, ...]
    dominated: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _dominates(left: HarnessTrial, right: HarnessTrial) -> bool:
    """Pareto dominance without an invented weighted score."""

    lvec, rvec = left.efficiency_vector, right.efficiency_vector
    return all(a <= b for a, b in zip(lvec, rvec, strict=True)) and any(
        a < b for a, b in zip(lvec, rvec, strict=True)
    )


def select_harness_trials(
    baseline: HarnessTrial,
    candidates: tuple[HarnessTrial, ...],
) -> HarnessSelection:
    """Keep only authority-equivalent, verifier-complete, non-dominated mechanisms.

    This function is research-only selection. It returns no promotion decision and has no callback
    into Airflow, GitHub, approvals, or candidate readiness. A faster harness with a different
    candidate SHA or authority projection is a refusal, not an optimization.
    """

    baseline.validate()
    if not baseline.evidence_complete or not baseline.verifier_passed:
        raise ValueError("baseline must have complete passing verifier evidence")

    names = [baseline.mechanism, *(trial.mechanism for trial in candidates)]
    if len(names) != len(set(names)):
        raise ValueError("harness trial mechanism names must be unique")

    admissible: list[HarnessTrial] = []
    refusals: list[str] = []
    for trial in candidates:
        trial.validate()
        reasons: list[str] = []
        if trial.candidate_sha != baseline.candidate_sha:
            reasons.append("candidate_sha_changed")
        if trial.authority_digest != baseline.authority_digest:
            reasons.append("authority_digest_changed")
        if not trial.evidence_complete:
            reasons.append("evidence_incomplete")
        if not trial.verifier_passed:
            reasons.append("verifier_failed")
        if reasons:
            refusals.append(f"{trial.mechanism}: {','.join(reasons)}")
            continue
        admissible.append(trial)

    pool = [baseline, *admissible]
    survivors: list[str] = []
    dominated: list[str] = []
    for trial in admissible:
        if any(other.mechanism != trial.mechanism and _dominates(other, trial) for other in pool):
            dominated.append(trial.mechanism)
        else:
            survivors.append(trial.mechanism)

    return HarnessSelection(
        baseline=baseline.mechanism,
        survivors=tuple(sorted(survivors)),
        refusals=tuple(sorted(refusals)),
        dominated=tuple(sorted(dominated)),
    )
