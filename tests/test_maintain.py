"""maintain: deterministic sigma classification, run loading, tier actions, orphan sweep."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from swfactory import maintain
from swfactory.config import Config
from swfactory.maintain import (
    Breach,
    detect,
    load_runs,
    remove_orphans,
    sweep_orphans,
)
from swfactory.models import AgentResult, Diagnosis

BANDS = {
    "window_runs": 20,
    "metrics": {
        "first_pass_test_rate": {"direction": "lower_is_bad"},
        "build_iterations": {"direction": "higher_is_bad"},
        "review_blockers": {"direction": "higher_is_bad"},
    },
    "tiers": [
        {"sigma": 1, "action": "log"},
        {"sigma": 2, "action": "diagnose"},
        {"sigma": 3, "action": "propose"},
    ],
}

# history: mean 10, sample stdev 2 (values 8,12 alternating, 8 samples)
HISTORY = [{"build_iterations": v, "first_pass_ci": v / 20} for v in (8, 12) * 4]


def _runs(latest: dict) -> list[dict]:
    return [latest, *HISTORY]


def _one(breaches: list[Breach], metric: str) -> Breach | None:
    hits = [b for b in breaches if b.metric == metric]
    assert len(hits) <= 1
    return hits[0] if hits else None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (10, None),  # on the mean
        (11.9, None),  # under 1σ
        (12.5, ("log", 1)),  # 1.25σ
        (14.5, ("diagnose", 2)),  # 2.25σ
        (17.0, ("propose", 3)),  # 3.27σ
        (40.0, ("propose", 3)),  # far beyond: highest tier wins, no higher tier exists
        (4.0, None),  # 3σ in the GOOD direction is not a breach
    ],
)
def test_detect_higher_is_bad_tiers(value: float, expected: tuple[str, int] | None) -> None:
    breach = _one(detect(_runs({"build_iterations": value}), BANDS), "build_iterations")
    if expected is None:
        assert breach is None
    else:
        assert (breach.action, breach.sigma) == expected
        assert breach.mean == pytest.approx(10) and breach.stdev == pytest.approx(2.138, abs=1e-3)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.5, None),
        (0.37, ("log", 1)),
        (0.27, ("diagnose", 2)),
        (0.15, ("propose", 3)),
        (0.9, None),  # high first-pass rate is good
    ],
)
def test_detect_lower_is_bad_tiers(value: float, expected: tuple[str, int] | None) -> None:
    # first_pass_test_rate resolves through the alias to first_pass_ci; history mean .5, stdev .107
    breach = _one(detect(_runs({"first_pass_ci": value}), BANDS), "first_pass_test_rate")
    if expected is None:
        assert breach is None
    else:
        assert (breach.action, breach.sigma) == expected


def test_detect_is_deterministic_and_typed() -> None:
    runs = _runs({"build_iterations": 15, "first_pass_ci": 0.1})
    first, second = detect(runs, BANDS), detect(runs, BANDS)
    assert first == second
    assert [b.metric for b in first] == ["first_pass_test_rate", "build_iterations"]
    assert all(isinstance(b, Breach) for b in first)


def test_detect_skips_metrics_with_too_few_samples_or_zero_stdev() -> None:
    few = [{"build_iterations": 50}, {"build_iterations": 1}, {"build_iterations": 3}]
    assert detect(few, BANDS) == []  # 2 history samples < 3
    flat = [{"build_iterations": 50}, *[{"build_iterations": 2} for _ in range(5)]]
    assert detect(flat, BANDS) == []  # stdev == 0
    # review_blockers never appears in the history -> skipped, not an error
    assert _one(detect(_runs({"review_blockers": 9}), BANDS), "review_blockers") is None
    assert detect([], BANDS) == [] and detect([{"build_iterations": 1}], BANDS) == []


def test_detect_explicit_key_and_nested_numbers() -> None:
    bands = {
        "metrics": {"cost": {"direction": "higher_is_bad", "key": "total_cost_usd"}},
        "tiers": [{"sigma": 2, "action": "diagnose"}],
    }
    runs = [{"numbers": {"total_cost_usd": 9.0}}, *[{"total_cost_usd": v} for v in (1, 2, 1, 2)]]
    (breach,) = detect(runs, bands)
    assert breach.metric == "cost" and breach.action == "diagnose" and breach.value == 9.0


# ---------------------------------------------------------------- load_runs


def _write_metrics(root: Path, issue: str, data: dict) -> Path:
    p = root / "docs" / "factory" / issue / "metrics.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def test_load_runs_newest_first_window_and_scripted_filter(tmp_path: Path) -> None:
    _write_metrics(
        tmp_path, "A", {"run_id": "a", "agent": "claude", "finished": "2026-01-01T00:00:00Z"}
    )
    _write_metrics(
        tmp_path,
        "B",
        {"run_id": "b", "agent": "claude", "timestamps": {"finished": "2026-03-01T00:00:00+00:00"}},
    )
    _write_metrics(tmp_path, "C", {"run_id": "c", "agent": "claude", "finished": 1_800_000_000})
    _write_metrics(
        tmp_path, "D", {"run_id": "d", "agent": "scripted", "finished": "2027-01-01T00:00:00Z"}
    )
    (tmp_path / "docs" / "factory" / "E").mkdir()
    (tmp_path / "docs" / "factory" / "E" / "metrics.json").write_text("not json")

    runs = load_runs(tmp_path, window=20)
    assert [r["run_id"] for r in runs] == ["c", "b", "a"]  # 1.8e9 s = 2027-01-15 > B > A
    assert [r["run_id"] for r in load_runs(tmp_path, window=2)] == ["c", "b"]
    assert "d" in [r["run_id"] for r in load_runs(tmp_path, window=20, include_scripted=True)]
    assert load_runs(tmp_path / "nowhere", window=5) == []


# ---------------------------------------------------------------- run() tiers


class FakeScm:
    kind = "local"

    def __init__(self) -> None:
        self.issues: list[dict] = []

    def fetch_issue(self, ref: str):  # pragma: no cover - protocol completeness
        raise NotImplementedError

    def publish(self, **kw):  # pragma: no cover - protocol completeness
        raise NotImplementedError

    def open_issue(self, *, title: str, body: str, labels) -> str:
        self.issues.append({"title": title, "body": body, "labels": list(labels)})
        return f"file://issue-{len(self.issues)}.md"


class FakeSandbox:
    name = "fake"
    workdir = "/w"

    def __init__(self) -> None:
        self.ensured = 0

    def ensure(self) -> None:
        self.ensured += 1

    def run(self, cmd, *, cwd=None, timeout_s=1800):  # pragma: no cover
        raise NotImplementedError

    def read(self, path):  # pragma: no cover
        raise FileNotFoundError(path)

    def write(self, path, content) -> None:  # pragma: no cover
        raise NotImplementedError

    def exists(self, path) -> bool:  # pragma: no cover
        return False

    def close(self) -> None:
        pass


class FakeAgent:
    kind = "scripted"

    def __init__(self, result: AgentResult) -> None:
        self.result = result
        self.calls: list[dict] = []

    def run(self, sb, **kw) -> AgentResult:
        self.calls.append(kw)
        return self.result


def _seed(root: Path, latest_iterations: float) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    for i, v in enumerate((8, 12) * 4):
        _write_metrics(
            root,
            f"H{i}",
            {
                "run_id": f"h{i}",
                "agent": "claude",
                "iterations": v,
                "finished": (base + timedelta(days=i)).isoformat(),
            },
        )
    _write_metrics(
        root,
        "L",
        {
            "run_id": "latest",
            "agent": "claude",
            "iterations": latest_iterations,
            "finished": (base + timedelta(days=30)).isoformat(),
        },
    )


def _bands_file(tmp_path: Path) -> Path:
    import yaml

    p = tmp_path / "bands.yaml"
    p.write_text(yaml.safe_dump(BANDS), encoding="utf-8")
    return p


def test_run_log_tier_only_prints(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "target"
    _seed(root, 12.5)
    scm = FakeScm()
    breaches = maintain.run(
        Config(issue="x"), scm=scm, agent=None, sb=None, bands_path=_bands_file(tmp_path), root=root
    )
    assert [b.action for b in breaches] == ["log"]
    assert "build_iterations" in capsys.readouterr().out
    assert scm.issues == [] and not (root / "docs/factory/incidents").exists()


def test_run_diagnose_tier_calls_agent_read_only_and_writes_incident(tmp_path: Path) -> None:
    root = tmp_path / "target"
    _seed(root, 14.5)
    diagnosis = Diagnosis(
        metric="build_iterations", hypothesis="flaky fixture", evidence=["run h3"]
    )
    agent = FakeAgent(AgentResult(agent="scripted", data=diagnosis.model_dump()))
    sb = FakeSandbox()
    now = datetime(2026, 9, 2, 12, tzinfo=UTC)
    breaches = maintain.run(
        Config(issue="x"),
        scm=FakeScm(),
        agent=agent,
        sb=sb,
        bands_path=_bands_file(tmp_path),
        root=root,
        now=now,
    )
    assert [b.action for b in breaches] == ["diagnose"]
    (call,) = agent.calls
    assert (
        call["stage"] == "diagnose"
        and call["schema"] is Diagnosis
        and call["issue_id"] == "maintain"
    )
    assert call["policy"].writes is False and "Edit" not in call["policy"].allowed_tools
    assert "build_iterations" in call["prompt"] and "h3" in call["prompt"]
    assert sb.ensured == 1
    incident = root / "docs/factory/incidents/2026-09-02-build_iterations.md"
    text = incident.read_text()
    assert "flaky fixture" in text and "run h3" in text and "2σ" in text


def test_run_propose_tier_opens_factory_issue_with_intent(tmp_path: Path) -> None:
    root = tmp_path / "target"
    _seed(root, 40)
    scm = FakeScm()
    agent = FakeAgent(AgentResult(agent="scripted", is_error=True, subtype="error_max_turns"))
    breaches = maintain.run(
        Config(issue="x"),
        scm=scm,
        agent=agent,
        sb=FakeSandbox(),
        bands_path=_bands_file(tmp_path),
        root=root,
    )
    assert [b.action for b in breaches] == ["propose"]
    (issue,) = scm.issues
    assert issue["labels"] == ["factory"]
    assert "build_iterations" in issue["title"] and "3σ" in issue["title"]
    assert "As a maintainer" in issue["body"] and "error_max_turns" in issue["body"]
    assert (root / "docs/factory/incidents").exists()


def test_run_propose_without_agent_still_opens_issue(tmp_path: Path) -> None:
    root = tmp_path / "target"
    _seed(root, 40)
    scm = FakeScm()
    maintain.run(
        Config(issue="x"), scm=scm, agent=None, sb=None, bands_path=_bands_file(tmp_path), root=root
    )
    assert len(scm.issues) == 1 and "no diagnosis" in scm.issues[0]["body"]


# ---------------------------------------------------------------- orphan sweep

NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def _ls(items: list[dict]) -> str:
    return json.dumps(items)


ME = "yossi.eliaz@incredibuild.com"


def _ls(items: list[dict]) -> str:
    base = {"status": "running", "created_by": ME, "created_at": "2026-09-01T00:00:00+00:00"}
    return json.dumps([{**base, **it} for it in items])


def test_sweep_orphans_requires_owner_and_factory_name() -> None:
    listing = _ls(
        [
            {"name": "swf-demo-1-aaaaaaaa"},  # mine, old, factory-named -> swept
            {"name": "swf-demo-1-bbbbbbbb", "created_by": "teammate@example.com"},  # NOT mine
            {"name": "swf-teammate-box"},  # mine but not a factory name (no run id) -> kept
            {"name": "prod-db"},  # not ours at all
            {"name": "swf-demo-1-cccccccc", "status": "deleted"},
            {"name": "swf-demo-1-dddddddd", "created_at": NOW.isoformat()},  # too young
        ]
    )
    assert sweep_orphans(listing, ttl_s=3600, now=NOW, owner=ME) == ["swf-demo-1-aaaaaaaa"]
    assert sweep_orphans(listing, ttl_s=3600, now=NOW, owner="") == []
    assert sweep_orphans(listing, ttl_s=3600, now=NOW, owner="someone@else.com") == []
    assert sweep_orphans("not json", 10, NOW, owner=ME) == []
    naive_now = NOW.replace(tzinfo=None)
    assert sweep_orphans(listing, 10, naive_now, owner=ME.upper()) == ["swf-demo-1-aaaaaaaa"]


def test_remove_orphans_refuses_foreign_names() -> None:
    calls: list[list[str]] = []

    def runner(argv):
        calls.append(list(argv))
        if argv[2] == "swf-demo-1-bad00000":
            raise RuntimeError("boom")
        return ""

    removed = remove_orphans(["swf-demo-1-aaaaaaaa", "prod-db", "swf-demo-1-bad00000"], runner)
    assert removed == ["swf-demo-1-aaaaaaaa"]
    assert [c[2] for c in calls] == ["swf-demo-1-aaaaaaaa", "swf-demo-1-bad00000"]  # prod-db never


def test_sweep_sandboxes_refuses_without_owner_and_never_uses_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def runner(argv):
        calls.append(list(argv))
        return _ls([{"name": "swf-old-1-aaaaaaaa"}]) if argv[1] == "ls" else ""

    monkeypatch.delenv(maintain.OWNER_ENV, raising=False)
    assert maintain.sweep_sandboxes(3600, runner=runner) == []
    assert calls == []  # refused before listing anything
    monkeypatch.setenv(maintain.OWNER_ENV, ME)
    assert maintain.sweep_sandboxes(3600, runner=runner) == ["swf-old-1-aaaaaaaa"]
    assert all("--all" not in c for c in calls)
    assert calls[0][:2] == ["islo", "ls"] and calls[1][:2] == ["islo", "rm"]
