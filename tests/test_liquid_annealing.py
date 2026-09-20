from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from swfactory import accepted_inputs, liquid_annealing, stage_registry, stages
from swfactory.blueprint import load
from swfactory.config import TargetContract, protected_for
from swfactory.liquid_annealing import LANES, AnnealingObservation, evaluate, merge_findings
from swfactory.models import Finding
from swfactory.models import TestResult as SwfTestResult


def _observation(**overrides: object) -> AnnealingObservation:
    values: dict[str, object] = {
        "round": 0,
        "changed_files": 4,
        "risky_files": 0,
        "blockers": 0,
        "majors": 0,
        "minors": 0,
        "nits": 0,
        "tests_green": True,
        "lane_coverage": 1.0,
        "stagnated": False,
        "exhausted": False,
    }
    values.update(overrides)
    return AnnealingObservation(**values)  # type: ignore[arg-type]


def _lanes() -> list[dict[str, object]]:
    return [
        {
            "lane": lane,
            "iteration": index,
            "verdict": "approve",
            "findings": 0,
            "blockers": 0,
        }
        for index, lane in enumerate(LANES, 1)
    ]


def test_clean_candidate_crystallizes_only_after_all_lanes_and_green_tests() -> None:
    state = evaluate(_observation())
    assert state.phase == "crystal"
    assert state.control_mode == "verify"
    assert state.crystallized is True
    assert state.actionable_findings == 0

    assert evaluate(_observation(lane_coverage=2 / 3)).crystallized is False
    red = evaluate(_observation(tests_green=False))
    assert red.phase == "critical"
    assert red.crystallized is False


def test_material_findings_keep_the_candidate_hot_and_stagnation_glasses_it() -> None:
    clean = evaluate(_observation())
    defective = evaluate(_observation(majors=2))
    assert defective.phase == "liquid"
    assert defective.control_mode == "anneal"
    assert defective.actionable_findings == 2
    assert defective.temperature > clean.temperature
    assert defective.beta < clean.beta

    glass = evaluate(_observation(majors=1, stagnated=True))
    assert glass.phase == "glass"
    assert glass.crystallized is False

    jammed = evaluate(_observation(tests_green=False, exhausted=True))
    assert jammed.phase == "jammed"


def test_interface_surface_increases_the_nucleation_barrier() -> None:
    small = evaluate(_observation(changed_files=1, risky_files=0))
    wide = evaluate(_observation(changed_files=20, risky_files=6))
    assert wide.surface_penalty > small.surface_penalty
    assert wide.nucleation_barrier > small.nucleation_barrier
    assert wide.crossing_probability < small.crossing_probability


def test_specialist_fan_in_is_deterministic_and_keeps_the_stronger_severity() -> None:
    lower = Finding(
        severity="minor",
        file="src/a.py",
        line=7,
        title="Same defect",
        detail="minor view",
    )
    higher = Finding(
        severity="major",
        file="src/a.py",
        line=7,
        title="same defect",
        detail="major view",
    )
    other = Finding(severity="nit", file="src/b.py", title="Naming", detail="small")

    merged = merge_findings([[lower, other], [higher]])
    assert [(item.file, item.severity) for item in merged] == [
        ("src/a.py", "major"),
        ("src/b.py", "nit"),
    ]
    assert merged[0].detail == "major view"


def test_one_round_calls_all_specialist_lanes_and_fans_them_in(monkeypatch: Any) -> None:
    calls: list[tuple[str, int, str]] = []
    ctx = SimpleNamespace(issue=SimpleNamespace(id="LIQ-1"))

    monkeypatch.setattr(stages, "_review_policy", lambda _ctx: "# liquid policy")
    monkeypatch.setattr(
        stages,
        "render_prompt",
        lambda _name, **kwargs: str(kwargs["review_policy"]),
    )
    monkeypatch.setattr(stages, "_nit_cap", lambda _ctx: 3)

    def fake_agent(_ctx: Any, stage: str, iteration: int, prompt: str, _schema: Any) -> Any:
        lane = next(lane for lane in LANES if f"\n{lane}\n" in prompt)
        calls.append((stage, iteration, lane))
        severity = "major" if lane == "risk" else "minor"
        return SimpleNamespace(
            data={
                "verdict": "approve",
                "findings": [
                    {
                        "severity": severity,
                        "file": "src/a.py",
                        "line": 7,
                        "title": "shared defect",
                        "detail": f"seen by {lane}",
                    }
                ],
            }
        )

    monkeypatch.setattr(stages, "_agent", fake_agent)
    findings, dropped, records = liquid_annealing._round(
        ctx,
        round_index=0,
        spec="spec",
        plan="plan",
        diff="diff",
    )

    assert calls == [
        ("review", 1, "correctness"),
        ("review", 2, "verification"),
        ("review", 3, "risk"),
    ]
    assert [record["lane"] for record in records] == list(LANES)
    assert dropped == 0
    assert len(findings) == 1 and findings[0].severity == "major"


class _State:
    def read_control(self, name: str) -> str:
        assert name == "base"
        return "base-sha\n"


class _Ctx:
    art = "docs/factory/LIQ-1"

    def __init__(self, *, max_review_fixes: int) -> None:
        self.state = _State()
        self.issue = SimpleNamespace(id="LIQ-1")
        self.cfg = SimpleNamespace(max_review_fixes=max_review_fixes, max_build_iterations=4)
        self.blueprint = SimpleNamespace(review=SimpleNamespace(nit_cap=3))
        self.written: dict[str, str] = {}

    def read_artifact(self, path: str) -> str:
        if path.endswith("plan.md"):
            return "# Plan\n"
        raise AssertionError(path)

    def write_artifact(self, path: str, content: str) -> None:
        self.written[path] = content


def _stage_harness(monkeypatch: Any) -> None:
    monkeypatch.setattr(stages, "_done", lambda *_args: None)
    monkeypatch.setattr(stages, "_assert_workspace_head", lambda *_args: "head")
    monkeypatch.setattr(stages, "_read_or", lambda *_args: "# Spec\n")
    monkeypatch.setattr(stages, "_exclude", lambda *_args: ":!docs/factory/LIQ-1")
    monkeypatch.setattr(stages, "_plan_fidelity", lambda *_args: [])
    monkeypatch.setattr(stages, "_protected", lambda *_args: "tests/")
    monkeypatch.setattr(stages, "_summary_line", lambda *_args: "relax material defect")
    monkeypatch.setattr(stages, "commit", lambda *_args, **_kwargs: "fixed-head")
    monkeypatch.setattr(
        stages,
        "run_tests",
        lambda *_args: (SwfTestResult(passed=1, exit_code=0), "ok"),
    )
    monkeypatch.setattr(
        stages,
        "_agent",
        lambda *_args, **_kwargs: SimpleNamespace(data={"summary": "relax material defect"}),
    )
    monkeypatch.setattr(stages, "render_prompt", lambda *_args, **_kwargs: "fix prompt")
    monkeypatch.setattr(
        stages,
        "_sh",
        lambda _ctx, command, **_kwargs: "src/a.py\n" if "--name-only" in command else "diff",
    )
    monkeypatch.setattr(liquid_annealing, "_initial_tests_green", lambda _ctx: True)


def test_liquid_packs_review_context_once_per_round_and_reuses_it_across_lanes(monkeypatch: Any) -> None:
    from swfactory import harness_efficiency

    ctx = _Ctx(max_review_fixes=0)
    _stage_harness(monkeypatch)
    packed = SimpleNamespace(
        prompt_text="PACKED REVIEW CONTEXT",
        source_bytes=20_000,
        prompt_bytes=1_000,
        estimated_replayed_bytes_avoided=57_000,
        handle="diff:sha256:" + "a" * 64,
        fanout=len(LANES),
    )
    pack_calls: list[tuple[str, str, int]] = []
    round_diffs: list[str] = []

    def fake_pack(_ctx: Any, *, diff: str, base_sha: str, head_sha: str, fanout: int) -> Any:
        assert diff == "diff"
        assert base_sha == "base-sha"
        assert head_sha == "head"
        pack_calls.append((base_sha, head_sha, fanout))
        return packed

    def fake_round(
        _ctx: Any,
        *,
        round_index: int,
        diff: str,
        **_kwargs: Any,
    ) -> tuple[list[Finding], int, list[dict[str, object]]]:
        assert round_index == 0
        round_diffs.append(diff)
        return [], 0, _lanes()

    monkeypatch.setattr(harness_efficiency, "pack_review_diff", fake_pack)
    monkeypatch.setattr(liquid_annealing, "_round", fake_round)

    result = liquid_annealing._run_annealed_review(ctx)  # type: ignore[arg-type]

    assert result.status == "ok"
    assert pack_calls == [("base-sha", "head", len(LANES))]
    assert round_diffs == ["PACKED REVIEW CONTEXT"]
    assert result.numbers["review_context_source_bytes"] == 20_000
    assert result.numbers["review_context_prompt_bytes"] == 1_000
    assert result.numbers["review_context_replayed_bytes_avoided"] == 57_000


def test_stage_relaxes_major_retests_and_crystallizes(monkeypatch: Any) -> None:
    ctx = _Ctx(max_review_fixes=1)
    _stage_harness(monkeypatch)
    major = Finding(
        severity="major",
        file="src/a.py",
        title="material defect",
        detail="repair me",
    )

    def fake_round(
        _ctx: Any,
        *,
        round_index: int,
        **_kwargs: Any,
    ) -> tuple[list[Finding], int, list[dict[str, object]]]:
        return ([major] if round_index == 0 else []), 0, _lanes()

    monkeypatch.setattr(liquid_annealing, "_round", fake_round)
    result = liquid_annealing._run_annealed_review(ctx)  # type: ignore[arg-type]

    assert result.status == "ok"
    assert result.numbers["fixes"] == 1
    assert result.numbers["crystallized"] == 1
    review = json.loads(ctx.written[f"{ctx.art}/review.json"])
    annealing = json.loads(ctx.written[f"{ctx.art}/annealing.json"])
    assert review["findings"] == []
    assert [step["state"]["phase"] for step in annealing["trace"]] == ["liquid", "crystal"]
    assert annealing["final"]["crystallized"] is True


def test_exhausted_major_blocks_even_without_a_review_blocker(monkeypatch: Any) -> None:
    ctx = _Ctx(max_review_fixes=0)
    _stage_harness(monkeypatch)
    major = Finding(
        severity="major",
        file="src/a.py",
        title="material defect",
        detail="still open",
    )
    monkeypatch.setattr(
        liquid_annealing,
        "_round",
        lambda *_args, **_kwargs: ([major], 0, _lanes()),
    )

    result = liquid_annealing._run_annealed_review(ctx)  # type: ignore[arg-type]
    assert result.status == "blocked"
    assert result.numbers["blockers"] == 0
    assert result.numbers["major"] == 1
    assert result.numbers["crystallized"] == 0


def test_liquid_policy_is_the_managed_annealing_opt_in(monkeypatch: Any) -> None:
    liquid = load("liquid")
    default = load("factory")
    assert liquid.review.policy == "REVIEW_LIQUID.md"
    assert default.review.policy == "REVIEW.md"
    assert accepted_inputs.packaged_review_policy_digest(liquid) is not None

    liquid_marker = object()
    default_marker = object()
    monkeypatch.setattr(liquid_annealing, "review", lambda _ctx: liquid_marker)
    monkeypatch.setattr(stages, "review", lambda _ctx: default_marker)

    dispatch = stage_registry.resolve("review")
    assert dispatch(SimpleNamespace(blueprint=liquid)) is liquid_marker
    assert dispatch(SimpleNamespace(blueprint=default)) is default_marker


def test_annealing_control_plane_is_selfhost_protected() -> None:
    root = Path(__file__).resolve().parents[1]
    contract = TargetContract.parse((root / "factory.toml").read_text(encoding="utf-8"))
    protected = (
        "REVIEW_LIQUID.md",
        "src/swfactory/stage_registry.py",
        "src/swfactory/liquid_annealing.py",
        "src/swfactory/phase_control.py",
        "config/phase-control.yaml",
    )
    for stage in ("build", "fix"):
        for path in protected:
            assert path in protected_for(contract, stage), f"{path} writable during {stage}"
