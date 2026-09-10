from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from swfactory import work_stage
from swfactory.candidate_readiness import CandidateNotReady, build_manifest
from swfactory.capability_inventory import load_inventory, support_matrix
from swfactory.idempotency import (
    MutationOutcome,
    OperationIdentityConflict,
    OperationJournal,
    OperationRef,
)
from swfactory.models import AgentResult, Plan, PlanTask


def _sha(ch: str) -> str:
    return ch * 40


def _digest(ch: str) -> str:
    return "sha256:" + ch * 64


def test_capability_inventory_has_explicit_support_boundaries() -> None:
    document = load_inventory(Path("config/capability-inventory.json"))
    matrix = support_matrix(document)

    assert matrix["airflow.lifecycle"] == "supported"
    assert matrix["mutation.github"] == "supported"
    assert matrix["workgraph.serial"] == "supported"
    assert matrix["workgraph.provider-fork"] == "experimental"
    assert matrix["factory.generations"] == "experimental"
    experimental = [row for row in document["claims"] if row["state"] == "experimental"]
    assert experimental
    assert all(str(row.get("follow_up") or "").strip() for row in experimental)


def test_candidate_readiness_is_bound_to_exact_head_base_and_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "core.txt"
    artifact.write_text("green\n", encoding="utf-8")
    manifest = build_manifest(
        head_sha=_sha("a"),
        base_sha=_sha("b"),
        tested_sha=_sha("c"),
        required=[("core", artifact)],
    )

    assert manifest.digest().startswith("sha256:")
    assert manifest.canonical_dict()["checks"][0]["head_sha"] == _sha("a")

    artifact.unlink()
    with pytest.raises(CandidateNotReady, match="does not exist"):
        build_manifest(
            head_sha=_sha("a"),
            base_sha=_sha("b"),
            tested_sha=_sha("c"),
            required=[("core", artifact)],
        )


def test_operation_key_cannot_be_reused_for_different_intent(tmp_path: Path) -> None:
    journal = OperationJournal(tmp_path / "ops.db")
    ref = OperationRef("cell_one", 1, "github_publish", "publish:one")
    try:
        result = journal.execute(ref, lambda: {"url": "one"}, intent_digest=_digest("a"))
        assert result == {"url": "one"}

        with pytest.raises(OperationIdentityConflict, match="divergent intent"):
            journal.execute(ref, lambda: {"url": "two"}, intent_digest=_digest("b"))

        stale_identity = OperationRef("cell_other", 1, "github_publish", "publish:one")
        with pytest.raises(OperationIdentityConflict, match="already bound"):
            journal.execute(stale_identity, lambda: {"url": "other"}, intent_digest=_digest("a"))
    finally:
        journal.close()


def test_ambiguous_effect_becomes_committed_only_after_observation(tmp_path: Path) -> None:
    journal = OperationJournal(tmp_path / "ops.db")
    ref = OperationRef("cell_one", 1, "github_issue", "issue:one")
    try:
        with pytest.raises(RuntimeError, match="lost response"):
            journal.execute(
                ref,
                lambda: (_ for _ in ()).throw(RuntimeError("lost response")),
                replay_safe=True,
                intent_digest=_digest("a"),
            )
        assert journal.get(ref.key)["state"] == "in_doubt"

        observed = journal.observe(
            ref,
            lambda: MutationOutcome(
                "committed",
                {"url": "https://example.invalid/issues/1"},
                {"marker": "issue:one"},
                "remote marker proves the effect",
            ),
        )
        assert observed.status == "committed"
        assert journal.get(ref.key)["state"] == "committed"
        assert journal.get(ref.key)["result"]["url"].endswith("/1")
    finally:
        journal.close()


class _State:
    def __init__(self) -> None:
        self.control: dict[str, str] = {}

    def has_control(self, name: str) -> bool:
        return name in self.control

    def read_control(self, name: str) -> str:
        return self.control[name]

    def write_control(self, name: str, value: str) -> None:
        self.control[name] = value


class _Ctx:
    def __init__(self) -> None:
        self.state = _State()
        self.issue = SimpleNamespace(id="42")
        self.art = "docs/factory/42"
        self.artifacts: dict[str, str] = {}
        self.sb = SimpleNamespace(run=lambda _cmd: SimpleNamespace(ok=True))

    def write_artifact(self, path: str, content: str) -> None:
        self.artifacts[path] = content


def test_workgraph_retry_resumes_checkpoint_and_uses_new_attempt_identity(monkeypatch) -> None:
    plan = Plan(
        files=["a.py", "b.py"],
        steps=["a then b"],
        tests=["pytest"],
        work=[
            PlanTask(id="a", title="A", files=["a.py"], parallel_safe=True),
            PlanTask(id="b", title="B", depends_on=["a"], files=["b.py"], parallel_safe=True),
        ],
    )
    ctx = _Ctx()
    current = {"head": "h0"}
    changed: dict[tuple[str, str], str] = {}
    calls: list[tuple[int, str]] = []
    fail_b_once = {"value": True}

    monkeypatch.setattr(work_stage.stages, "_assert_workspace_head", lambda _ctx, _phase: current["head"])

    # Template-aware, not a blanket "base". `_node_prompt` renders `build_node` for the per-node
    # instruction that used to be a Python literal (#2098), and the worker below keys on the node id
    # that instruction carries. A stub that returned "base" for every template erased the id, the
    # trigger never fired, and the retry this test exists to exercise never happened.
    def _render(stage: str, **vars: object) -> str:
        return f"Node: `{vars['node_id']}`" if stage == "build_node" else "base"

    monkeypatch.setattr(work_stage.stages, "render_prompt", _render)
    monkeypatch.setattr(work_stage.stages, "_protected", lambda *_args, **_kwargs: "(none)")

    def agent(_ctx, _stage, iteration, prompt, _schema):
        calls.append((iteration, prompt))
        if "Node: `b`" in prompt and fail_b_once["value"]:
            fail_b_once["value"] = False
            raise RuntimeError("worker disappeared")
        return AgentResult(agent="scripted", data={"summary": "ok"})

    def commit(_ctx, *, stage: str, msg: str) -> str:
        del msg
        node = stage.split(":", 1)[1]
        before = current["head"]
        after = before + "-" + node
        current["head"] = after
        changed[(before, after)] = f"{node}.py\n"
        return after

    def sh(_ctx, command: str, *, timeout_s: int = 600) -> str:
        del timeout_s
        if command.startswith("git diff --name-only "):
            span = command.split()[3]
            before, after = span.split("..", 1)
            return changed[(before, after)]
        return ""

    def restore(_ctx, head: str) -> None:
        current["head"] = head

    monkeypatch.setattr(work_stage.stages, "_agent", agent)
    monkeypatch.setattr(work_stage.stages, "commit", commit)
    monkeypatch.setattr(work_stage.stages, "_sh", sh)
    monkeypatch.setattr(work_stage, "_restore", restore)

    with pytest.raises(RuntimeError, match="worker disappeared"):
        work_stage._execute_nodes(ctx, plan, "spec", "plan", work_stage._open_progress(ctx, plan))

    checkpoint = json.loads(ctx.state.read_control("workgraph-progress.json"))
    assert [row["node_id"] for row in checkpoint["nodes"]] == ["a"]
    assert checkpoint["agent_calls"] == 2
    assert checkpoint["attempt"] is None, "the lost attempt is struck, not left to be mistaken for a commit"
    assert current["head"] == "h0-a"

    calls_before_retry = list(calls)
    progress = work_stage._open_progress(ctx, plan)
    work_stage._execute_nodes(ctx, plan, "spec", "plan", progress)

    assert calls_before_retry[0][0] == 1
    assert calls_before_retry[1][0] == 2
    assert calls[-1][0] == 3
    assert sum("Node: `a`" in prompt for _, prompt in calls) == 1
    assert sum("Node: `b`" in prompt for _, prompt in calls) == 2
    assert progress["agent_calls"] == 3
    assert progress["head"] == "h0-a-b"
    assert [row["node_id"] for row in progress["nodes"]] == ["a", "b"]
