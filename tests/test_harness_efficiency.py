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
    pack_review_diff,
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
    assert (
        pack_failure_observation(
            ctx,
            command="pytest -q",
            exit_code=1,
            timed_out=False,
            stdout="one failed\n",
            stderr="",
        )
        is None
    )
    assert ctx.sb.files == {}


def test_long_failure_is_archived_exactly_and_compacted_with_stable_handle(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    stdout = "EARLY-EVIDENCE\n" + "".join(f"noise {i:04d} lorem ipsum dolor sit amet\n" for i in range(600))
    stdout += "FAILED tests/test_widget.py::test_edge - AssertionError: expected 7\n"
    stderr = "Traceback (most recent call last):\n  line 1\nAssertionError: expected 7\n"

    packed = pack_failure_observation(
        ctx,
        command="pytest -q",
        exit_code=1,
        timed_out=False,
        stdout=stdout,
        stderr=stderr,
    )

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

    public = json.loads(ctx.state.read_artifact(f"{ctx.art}/harness-observations/{packed.ref.sha256}.json"))
    assert public["remote_model_used"] is False
    assert public["raw_log_committed"] is False
    assert public["command_sha256"] == packed.ref.command_sha256
    assert public["exit_code"] == 1
    assert public["timed_out"] is False
    assert public["saved_bytes"] == packed.saved_bytes
    assert all("text" not in quote for quote in public["quotes"])


def test_sensitive_full_log_stays_host_only_and_never_enters_agent_prompt(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    token = "ghp_" + "a" * 36
    stdout = f"token={token}\n" + "noise\n" * 1800
    stdout += "FAILED tests/test_widget.py::test_edge - AssertionError: expected 7\n"

    packed = pack_failure_observation(
        ctx,
        command="pytest -q",
        exit_code=1,
        timed_out=False,
        stdout=stdout,
        stderr="",
    )

    assert packed is not None
    assert packed.ref.sandbox_path is None
    assert packed.ref.sensitivity_kinds == ("github-token",)
    assert token in ctx.state.read_artifact(packed.ref.state_path)
    assert token not in packed.prompt_text
    assert "host-only" in packed.prompt_text
    assert not any(path.startswith(".factory/observations/") for path in ctx.sb.files)

    public = json.loads(ctx.state.read_artifact(f"{ctx.art}/harness-observations/{packed.ref.sha256}.json"))
    assert public["sensitive"] is True
    assert public["sensitivity_kinds"] == ["github-token"]
    assert public["raw_log_committed"] is False


def test_same_observation_has_same_handle_and_archive_identity(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    stdout = "x" * 7000 + "\nFAILED deterministic\n"

    first = pack_failure_observation(
        ctx,
        command="pytest -q",
        exit_code=1,
        timed_out=False,
        stdout=stdout,
        stderr="",
    )
    second = pack_failure_observation(
        ctx,
        command="pytest -q",
        exit_code=1,
        timed_out=False,
        stdout=stdout,
        stderr="",
    )

    assert first is not None and second is not None
    assert first.ref == second.ref
    assert first.prompt_text == second.prompt_text


def test_quote_verifier_refuses_wrong_text() -> None:
    source = "one\ntwo\nthree"
    quote = Quote(start_line=2, end_line=2, text="TWO")

    with pytest.raises(ObservationIntegrityError, match="does not match"):
        verify_quotes(source, (quote,))


def test_exact_page_is_one_based_and_lossless() -> None:
    source = "\n".join(f"line-{i}" for i in range(1, 21))
    assert exact_page(source, start_line=7, limit=3) == "line-7\nline-8\nline-9"
    with pytest.raises(ValueError):
        exact_page(source, start_line=0, limit=3)


def _large_review_diff() -> str:
    header = "diff --git a/src/widget.py b/src/widget.py\n--- a/src/widget.py\n+++ b/src/widget.py\n@@ -1,2 +1,602 @@\n"
    body = "".join(f"+generated review line {i:04d} with deterministic content\n" for i in range(600))
    second = (
        "diff --git a/tests/test_widget.py b/tests/test_widget.py\n"
        "--- a/tests/test_widget.py\n"
        "+++ b/tests/test_widget.py\n"
        "@@ -1 +1,3 @@\n"
        "+def test_widget():\n"
        "+    assert True\n"
    )
    return header + body + second


def test_small_review_diff_keeps_the_existing_prompt_contract(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    diff = "diff --git a/a.py b/a.py\n@@ -1 +1 @@\n-old\n+new\n"

    assert (
        pack_review_diff(
            ctx,
            diff=diff,
            base_sha="a" * 40,
            head_sha="b" * 40,
            fanout=3,
        )
        is None
    )
    assert ctx.sb.files == {}


def test_large_review_diff_is_exactly_archived_and_packed_once_for_fanout(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    diff = _large_review_diff()

    packed = pack_review_diff(
        ctx,
        diff=diff,
        base_sha="a" * 40,
        head_sha="b" * 40,
        fanout=3,
    )

    assert packed is not None
    assert packed.handle == f"diff:sha256:{packed.sha256}"
    assert ctx.state.read_artifact(packed.state_path) == diff
    assert ctx.sb.files[packed.sandbox_path] == diff
    assert packed.prompt_bytes < packed.source_bytes
    assert packed.estimated_replayed_bytes_avoided == packed.saved_bytes_per_prompt * 3
    assert [item.path for item in packed.files] == ["src/widget.py", "tests/test_widget.py"]
    assert packed.files[0].hunks == 1
    assert packed.files[0].added == 600
    assert "navigation, not evidence" in packed.prompt_text.lower()
    assert packed.sandbox_path in packed.prompt_text

    public = json.loads(ctx.state.read_artifact(f"{ctx.art}/harness-review-context/{packed.sha256}.json"))
    assert public["authority"] == "review-navigation-only"
    assert public["raw_diff_committed"] is False
    assert public["remote_model_used"] is False
    assert public["fanout"] == 3
    assert public["estimated_replayed_bytes_avoided"] == packed.estimated_replayed_bytes_avoided
    assert "generated review line" not in json.dumps(public)


def test_review_diff_handle_is_stable_and_archive_is_reused(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    diff = _large_review_diff()

    first = pack_review_diff(
        ctx,
        diff=diff,
        base_sha="a" * 40,
        head_sha="b" * 40,
        fanout=3,
    )
    second = pack_review_diff(
        ctx,
        diff=diff,
        base_sha="a" * 40,
        head_sha="b" * 40,
        fanout=3,
    )

    assert first is not None and second is not None
    assert first.handle == second.handle
    assert first.prompt_text == second.prompt_text
    assert first.files == second.files


def test_secret_shaped_review_diff_is_not_copied_into_recall_scratch(tmp_path: Path) -> None:
    ctx = FakeCtx(tmp_path)
    token = "ghp_" + "a" * 36
    diff = _large_review_diff() + f"\n+token={token}\n"

    packed = pack_review_diff(
        ctx,
        diff=diff,
        base_sha="a" * 40,
        head_sha="b" * 40,
        fanout=3,
    )

    assert packed is None
    assert not any(path.startswith(".factory/observations/review-diff-") for path in ctx.sb.files)
    assert not any("harness-review-context" in path for path in ctx.sb.files)


def test_compaction_has_no_remote_model_client() -> None:
    source = Path(__file__).parents[1] / "src" / "swfactory" / "harness_efficiency.py"
    text = source.read_text(encoding="utf-8")
    assert "requests." not in text
    assert "httpx." not in text
    assert "anthropic" not in text.lower()
    assert "openai" not in text.lower()


def _trial(
    mechanism: str,
    *,
    sha: str = "a" * 40,
    authority: str = "sha256:" + "b" * 64,
    evidence: str = "sha256:" + "c" * 64,
    prompt: int = 1000,
    cost: int = 1000,
    wall: int = 1000,
    turns: int = 10,
    complete: bool = True,
    passed: bool = True,
):
    from swfactory.harness_efficiency import HarnessTrial

    return HarnessTrial(
        mechanism=mechanism,
        candidate_sha=sha,
        authority_digest=authority,
        evidence_digest=evidence,
        prompt_bytes=prompt,
        cost_microusd=cost,
        wall_ms=wall,
        model_turns=turns,
        evidence_complete=complete,
        verifier_passed=passed,
    )


def test_harness_selection_refuses_speedups_that_change_the_answer() -> None:
    from swfactory.harness_efficiency import select_harness_trials

    baseline = _trial("baseline")
    fast_wrong_sha = _trial("fast-wrong-sha", sha="d" * 40, prompt=10, cost=10, wall=10, turns=1)
    fast_wrong_authority = _trial(
        "fast-wrong-authority",
        authority="sha256:" + "e" * 64,
        prompt=10,
        cost=10,
        wall=10,
        turns=1,
    )

    selected = select_harness_trials(baseline, (fast_wrong_sha, fast_wrong_authority))

    assert selected.survivors == ()
    assert any("candidate_sha_changed" in item for item in selected.refusals)
    assert any("authority_digest_changed" in item for item in selected.refusals)


def test_harness_selection_refuses_incomplete_or_red_evidence() -> None:
    from swfactory.harness_efficiency import select_harness_trials

    baseline = _trial("baseline")
    incomplete = _trial("incomplete", complete=False, prompt=1)
    red = _trial("red", passed=False, prompt=1)

    selected = select_harness_trials(baseline, (incomplete, red))

    assert selected.survivors == ()
    assert any("evidence_incomplete" in item for item in selected.refusals)
    assert any("verifier_failed" in item for item in selected.refusals)


def test_harness_selection_uses_pareto_survival_not_fake_weighted_scoring() -> None:
    from swfactory.harness_efficiency import select_harness_trials

    baseline = _trial("baseline", prompt=1000, cost=1000, wall=1000, turns=10)
    strictly_better = _trial("strictly-better", prompt=800, cost=900, wall=900, turns=8)
    token_specialist = _trial("token-specialist", prompt=600, cost=1100, wall=1000, turns=9)
    dominated = _trial("dominated", prompt=900, cost=1000, wall=1000, turns=10)

    selected = select_harness_trials(
        baseline,
        (dominated, token_specialist, strictly_better),
    )

    assert selected.survivors == ("strictly-better", "token-specialist")
    assert selected.dominated == ("dominated",)


def test_harness_selection_is_completion_order_independent() -> None:
    from swfactory.harness_efficiency import select_harness_trials

    baseline = _trial("baseline")
    trials = (
        _trial("a", prompt=800, cost=900, wall=900, turns=9),
        _trial("b", prompt=700, cost=1100, wall=900, turns=9),
        _trial("c", prompt=900, cost=1200, wall=1100, turns=12),
    )

    first = select_harness_trials(baseline, trials)
    second = select_harness_trials(baseline, tuple(reversed(trials)))

    assert first == second
