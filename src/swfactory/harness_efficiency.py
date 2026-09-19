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
    r"(?i)(traceback|assertionerror|\\bfailed\\b|\\bfailure\\b|\\berror\\b|\\bpanic\\b|"
    r"\\bexception\\b|caused by|##\\[error\\]|^E\\s+|^error:)"
)


class ObservationIntegrityError(ValueError):
    """An observation or reducer receipt no longer matches its archived source."""


@dataclass(frozen=True)
class ObservationRef:
    handle: str
    sha256: str
    state_path: str
    sandbox_path: str
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


def _source(stdout: str, stderr: str) -> str:
    """One exact, unambiguous text source for both process streams."""

    return f"=== stdout ===\\n{stdout}\\n=== stderr ===\\n{stderr}"


def _legacy_tail(stdout: str, stderr: str) -> str:
    """The pre-ObservationPack repair context, retained as the fallback contract."""

    return (stdout[-_STDOUT_TAIL:] + "\\n" + stderr[-_STDERR_TAIL:]).strip()


def _archive(ctx: Ctx, source: str) -> ObservationRef:
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

    ctx.sb.write(sandbox_path, source)
    lines = len(source.splitlines())
    return ObservationRef(
        handle=f"obs:sha256:{digest}",
        sha256=digest,
        state_path=state_path,
        sandbox_path=sandbox_path,
        size_bytes=len(source.encode("utf-8")),
        lines=lines,
    )


def exact_page(source: str, *, start_line: int = 1, limit: int = 200) -> str:
    """Return an exact 1-based line page for deterministic recall."""

    if start_line < 1 or limit < 1:
        raise ValueError("start_line and limit must be positive")
    lines = source.splitlines()
    return "\\n".join(lines[start_line - 1 : start_line - 1 + limit])


def verify_quotes(source: str, quotes: tuple[Quote, ...]) -> None:
    """Fail unless every quotation is exactly the archived source slice it claims."""

    lines = source.splitlines()
    for quote in quotes:
        if quote.start_line < 1 or quote.end_line < quote.start_line or quote.end_line > len(lines):
            raise ObservationIntegrityError(
                f"quote range {quote.start_line}-{quote.end_line} is outside a {len(lines)} line observation"
            )
        exact = "\\n".join(lines[quote.start_line - 1 : quote.end_line])
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
            error_ranges.append(
                (max(0, index - _CONTEXT_LINES), min(len(lines), index + _CONTEXT_LINES + 1))
            )

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
        text = "\\n".join(lines[start:end])
        cost = len(text.encode("utf-8")) + 48
        if cost > _PROMPT_BUDGET_BYTES:
            continue
        if used + cost > _PROMPT_BUDGET_BYTES and selected:
            continue
        selected.append((start, end))
        used += cost

    quotes = tuple(
        Quote(start_line=start + 1, end_line=end, text="\\n".join(lines[start:end]))
        for start, end in _merge(selected)
    )
    verify_quotes(source, quotes)
    return quotes


def _reduced_prompt(ref: ObservationRef, quotes: tuple[Quote, ...]) -> str:
    header = (
        "TEST OUTPUT COMPACTED LOCALLY; THE ARCHIVE IS AUTHORITATIVE.\\n"
        f"handle: {ref.handle}\\n"
        f"exact source: {ref.sandbox_path}\\n"
        f"sha256: {ref.sha256}\\n"
        f"source: {ref.size_bytes} bytes, {ref.lines} lines\\n"
        "Use Read on the exact source with an offset/limit when an omitted line matters. "
        "Every excerpt below was verified byte-for-byte against that source.\\n"
    )
    blocks = [
        f"\\n[exact lines {quote.start_line}-{quote.end_line}]\\n{quote.text}"
        for quote in quotes
    ]
    return (header + "".join(blocks)).strip()


def pack_failure_observation(ctx: Ctx, *, stdout: str, stderr: str) -> PackedObservation | None:
    """Archive a truncated failure and return a smaller, exactly-recallable repair observation.

    Small failures retain the legacy behavior exactly and return None. The optimization engages
    only when the old code would have discarded output.
    """

    if len(stdout) <= _STDOUT_TAIL and len(stderr) <= _STDERR_TAIL:
        return None

    source = _source(stdout, stderr)
    ref = _archive(ctx, source)
    legacy = _legacy_tail(stdout, stderr)
    quotes = _quote_ranges(source)
    reduced = _reduced_prompt(ref, quotes) if quotes else ""

    if not reduced or len(reduced.encode("utf-8")) >= len(legacy.encode("utf-8")):
        prompt = (
            f"FULL TEST OUTPUT ARCHIVED AS {ref.handle} AT {ref.sandbox_path} "
            f"(sha256:{ref.sha256}). Use Read for omitted context.\\n\\n{legacy}"
        ).strip()
        mode: Literal["reduced", "legacy-with-handle"] = "legacy-with-handle"
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
        json.dumps(internal, indent=2, sort_keys=True) + "\\n",
    )

    public = {
        "schema_version": 1,
        "handle": ref.handle,
        "sha256": ref.sha256,
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
        json.dumps(public, indent=2, sort_keys=True) + "\\n",
    )
    return packed
