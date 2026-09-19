"""Evidence-preserving harness-efficiency contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from swfactory.harness_efficiency import (
    ObservationIntegrityError,
    Quote,
    exact_page,
    pack_failure_observation,
    verify_quotes,
)
from swfactory.state import RunState


class FakeSandbox:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def write(self, path: str, content: str) -> None:
        self.files[path] = content

    def read(self, path: str) -> str:
        return self.files[path]


class FakeCtx:
    art = "docs/factory/ISSUE-1"

    def __init__(self, root: Path) -> None:
        self.state = RunState(root / "run")
        self.sb = FakeSandbox()

    def write_artifact(self, path: str, content: str) -> None:
        self.state.write_artifact(path, content)
        self.sb.write(path, content)


def test_small_failure_keeps_the_existing_prompt_contract(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    assert pack_failure_observation(ctx, stdout="one failed\\n", stderr="") is None
    assert ctx.sb.files == {}


def test_long_failure_is_archived_exactly_and_compacted_with_stable_handle(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    stdout = "EARLY-EVIDENCE\\n" + "".join(f"noise {i:04d} lorem ipsum dolor sit amet\\n" for i in range(600))
    stdout += "FAILED tests/test_widget.py::test_edge - AssertionError: expected 7\\n"
    stderr = "Traceback (most recent call last):\\n  line 1\\nAssertionError: expected 7\\n"

    packed = pack_failure_observation(ctx, stdout=stdout, stderr=stderr)

    assert packed is not None
    assert packed.ref.handle == f"obs:sha256:{packed.ref.sha256}"
    assert packed.ref.sandbox_path in ctx.sb.files
    source = ctx.state.read_artifact(packed.ref.state_path)
    assert source == ctx.sb.files[packed.ref.sandbox_path]
    assert "EARLY-EVIDENCE" in source
    assert "AssertionError: expected 7" in packed.prompt_text
    assert packed.ref.handle in packed.prompt_text
    assert packed.prompt_bytes < packed.legacy_bytes
    verify_quotes(source, packed.quotes)

    public = json.loads(
        ctx.state.read_artifact(f"{ctx.art}/harness-observations/{packed.ref.sha256}.json")
    )
    assert public["remote_model_used"] is False
    assert public["raw_log_committed"] is False
    assert public["saved_bytes"] == packed.saved_bytes
    assert all("text" not in quote for quote in public["quotes"])


def test_same_observation_has_same_handle_and_archive_identity(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    stdout = "x" * 7000 + "\\nFAILED deterministic\\n"

    first = pack_failure_observation(ctx, stdout=stdout, stderr="")
    second = pack_failure_observation(ctx, stdout=stdout, stderr="")

    assert first is not None and second is not None
    assert first.ref == second.ref
    assert first.prompt_text == second.prompt_text


def test_quote_verifier_refuses_wrong_text() -> None:
    source = "one\\ntwo\\nthree"
    quote = Quote(start_line=2, end_line=2, text="TWO")

    with pytest.raises(ObservationIntegrityError, match="does not match"):
        verify_quotes(source, (quote,))


def test_exact_page_is_one_based_and_lossless() -> None:
    source = "\\n".join(f"line-{i}" for i in range(1, 21))
    assert exact_page(source, start_line=7, limit=3) == "line-7\\nline-8\\nline-9"
    with pytest.raises(ValueError):
        exact_page(source, start_line=0, limit=3)


def test_compaction_has_no_remote_model_client() -> None:
    source = Path(__file__).parents[1] / "src" / "swfactory" / "harness_efficiency.py"
    text = source.read_text(encoding="utf-8")
    assert "requests." not in text
    assert "httpx." not in text
    assert "anthropic" not in text.lower()
    assert "openai" not in text.lower()
