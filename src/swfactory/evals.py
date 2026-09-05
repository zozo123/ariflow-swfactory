"""The eval suite: representative issues with their expected outcome, checked by machine.

WHY this exists: `CLAUDE.md`, the stage prompts, `REVIEW.md`, the skills and the hooks steer the
agent, so they deserve the regression testing code gets. One recorded demo run proves the
pipeline moves; it cannot tell you that a prompt edit stopped the reviewer from blocking an
untested function. Each eval in ``demo/evals/<slug>/`` is a real, tiny change to the demo target
whose front matter states the outcome in machine-checkable fields (see docs/evals.md), so a
suite run yields a pass rate and ``baseline_diff`` turns a drop into a red build.

What this module does NOT do: call a model, touch the network, or judge prose. ``check`` reads a
finished :class:`~swfactory.models.RunReport` plus the run's workdir; ``run_suite`` drives the
same pipeline the CLI drives, with the scripted agent by default so CI needs no keys. Exports are
verified statically (AST), never by importing agent-authored code on the orchestrator.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from swfactory import blueprint as blueprint_mod
from swfactory.blueprint import Blueprint
from swfactory.config import FACTORY_ROOT, Config, TargetContract
from swfactory.models import RunReport, StageError
from swfactory.runtime import ctx_for, run_id_for
from swfactory.stages import cli_approver, run_pipeline, setup

SUITE_DIR = "demo/evals"
BASELINE_NAME = "baseline.json"  # lives in the suite dir, so --suite moves both
ISSUE_NAME = "issue.md"  # one eval = one directory: the issue file plus its fixtures
EVAL_RUNS_DIR = Path(".factory") / "evals"  # gitignored scratch, one subdir per suite run


# ---------------------------------------------------------------- the eval and its expectations


class Expect(BaseModel):
    """The expected outcome of one eval, as it appears under ``expect:`` in the front matter.

    ``extra="forbid"``: a misspelt key would otherwise silently expect nothing, which is the one
    failure mode an eval suite must not have.
    """

    model_config = ConfigDict(extra="forbid")

    stages: list[str] = Field(default_factory=list)  # each must reach status "ok"
    tests_pass: bool | None = None
    max_build_iterations: int | None = None  # upper bound: fewer is never a failure
    max_review_fixes: int | None = None  # upper bound
    blockers: int | None = None  # exact count left by review/deliver
    label: str | None = None  # e.g. factory:blocked — must be on the delivered PR
    exports: list[str] = Field(default_factory=list)  # dotted: calc.average
    artifacts_contain: dict[str, list[str]] = Field(default_factory=dict)  # artifact -> substrings


class Eval(BaseModel):
    """One eval: the issue the factory is given, and what a good run must produce."""

    id: str
    title: str
    slug: str  # directory name, the human-readable handle in reports
    issue_path: Path
    fixtures_dir: Path
    expect: Expect

    @property
    def artifacts(self) -> str:
        """Where this eval's committed chain lands inside the run's workdir."""
        return Config.artifacts_dir(self.id)


class EvalOutcome(BaseModel):
    """One eval's verdict: whatever ``check`` returned (an empty list is a pass)."""

    id: str
    slug: str = ""
    failures: list[str] = Field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


# ---------------------------------------------------------------- loading


def parse_eval(path: Path) -> Eval:
    """Parse one ``issue.md`` (front matter + body) into an :class:`Eval`.

    The front matter is the same one ``scm.parse_issue_file`` reads for ``id``/``title``/``labels``
    (extra keys are ignored there), so a single file is both the issue the factory runs and the
    expectation it is judged against — they cannot drift apart.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ValueError(f"{path}: cannot read eval: {e}") from e
    if not text.startswith("---"):
        raise ValueError(f"{path}: no '---' front matter")
    parts = text.split("\n---", 2)
    if len(parts) < 2:
        raise ValueError(f"{path}: unterminated '---' front matter")
    try:
        meta = yaml.safe_load(parts[0].lstrip("-\n")) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"{path}: invalid front matter: {e}") from e
    if not isinstance(meta, dict) or "id" not in meta or "title" not in meta:
        raise ValueError(f"{path}: front matter needs 'id' and 'title'")
    if "expect" not in meta:
        raise ValueError(f"{path}: front matter needs an 'expect' block (see docs/evals.md)")
    try:
        expect = Expect.model_validate(meta["expect"])
    except ValueError as e:
        raise ValueError(f"{path}: invalid expect block: {e}") from e
    return Eval(
        id=str(meta["id"]),
        title=str(meta["title"]),
        slug=path.parent.name,
        issue_path=path.resolve(),
        fixtures_dir=path.parent.resolve(),
        expect=expect,
    )


def load_suite(suite_dir: str | Path = SUITE_DIR) -> list[Eval]:
    """Every ``<suite_dir>/*/issue.md``, in directory order.

    A broken eval raises instead of being skipped: an expectation that quietly vanishes from the
    suite is worse than a red build.
    """
    root = Path(suite_dir)
    paths = sorted(p for p in root.glob(f"*/{ISSUE_NAME}") if p.is_file())
    if not paths:
        raise ValueError(f"no evals found: {root}/*/{ISSUE_NAME}")
    evals = [parse_eval(p) for p in paths]
    seen = {e.id for e in evals}
    if len(seen) != len(evals):
        raise ValueError(f"duplicate eval ids in {root}")
    return evals


# ---------------------------------------------------------------- checking a finished run


def check(ev: Eval, report: RunReport, workdir: Path) -> list[str]:
    """Judge one finished run against ``ev.expect``; the returned strings are the failures.

    Pure and offline: ``report`` is the run's :class:`RunReport` and ``workdir`` the target
    checkout it left behind (artifact chain included). Empty list == this eval passed.
    """
    workdir = Path(workdir)
    exp = ev.expect
    status = {s.stage: s.status for s in report.stages}
    numbers = {s.stage: s.numbers for s in report.stages}
    out: list[str] = []

    for stage in exp.stages:
        if status.get(stage) != "ok":
            out.append(f"stage {stage}: expected ok, got {status.get(stage) or 'no record'}")
    if exp.tests_pass is not None and report.tests_passed != exp.tests_pass:
        out.append(f"tests_passed: expected {exp.tests_pass}, got {report.tests_passed}")
    out += _bound("build_and_test", "iterations", exp.max_build_iterations, numbers)
    out += _bound("review", "fixes", exp.max_review_fixes, numbers)

    if exp.blockers is not None:
        got = _blocker_count(numbers)
        if got is None:
            out.append(f"blockers: expected {exp.blockers}, no review or deliver record")
        elif got != exp.blockers:
            out.append(f"blockers: expected {exp.blockers}, got {got}")
    if exp.label is not None:
        labels = delivered_labels(report)
        if labels is None:
            out.append(f"label {exp.label}: no delivered PR to read labels from")
        elif exp.label not in labels:
            out.append(f"label {exp.label}: PR carries {labels or ['(none)']}")

    out += [msg for dotted in exp.exports if (msg := _check_export(workdir, dotted))]
    art = workdir / ev.artifacts
    for name, needles in exp.artifacts_contain.items():
        try:
            text = (art / name).read_text(encoding="utf-8")
        except OSError:
            out.append(f"artifact {name}: missing from {ev.artifacts}/")
            continue
        out += [f"artifact {name}: does not contain {n!r}" for n in needles if n not in text]
    return out


def _bound(
    stage: str, key: str, limit: int | None, numbers: dict[str, dict[str, float]]
) -> list[str]:
    """One ``max_*`` expectation: the stage must have run, and its counter stay within ``limit``.

    An upper bound, not an equality: a change that reaches the same outcome in fewer build
    iterations or review fixes is an improvement, and an eval must never punish it.
    """
    if limit is None:
        return []
    got = numbers.get(stage, {}).get(key)
    if got is None:
        return [f"{stage}.{key}: expected <= {limit}, stage did not record it"]
    if got > limit:
        return [f"{stage}.{key}: expected <= {limit}, got {got:g}"]
    return []


def _blocker_count(numbers: dict[str, dict[str, float]]) -> int | None:
    """Blockers the run ended with: ``deliver`` first (it is never skipped), then ``review``."""
    for stage in ("deliver", "review"):
        if "blockers" in numbers.get(stage, {}):
            return int(numbers[stage]["blockers"])
    return None


def delivered_labels(report: RunReport) -> list[str] | None:
    """Labels of the PR the run published, or ``None`` when they cannot be established.

    Read back from the local scm's ``pr.md`` when it is on disk: an eval that expects
    ``factory:blocked`` must fail when the labelling breaks, not when a copy of deliver's rule
    drifts. A GitHub PR url needs a token to read, so there the deliver counters stand in.
    """
    url = report.pr_url or ""
    if url.startswith("file://"):
        try:
            text = Path(url.removeprefix("file://")).read_text(encoding="utf-8")
        except OSError:
            text = ""
        for line in text.splitlines():
            if line.startswith("labels:"):
                labels = [p.strip() for p in line.removeprefix("labels:").split(",")]
                return [label for label in labels if label and label != "(none)"]
    nums = next((s.numbers for s in report.stages if s.stage == "deliver"), None)
    if nums is None:
        return None
    if nums.get("rejected"):
        return ["factory:rejected"]
    return ["factory:blocked"] if nums.get("blockers") else []


# ---------------------------------------------------------------- exports (static, never import)


def _source_root(workdir: Path) -> Path:
    """The target's package root from its ``factory.toml`` (``[paths].source``, default ``src``)."""
    try:
        contract = TargetContract.parse((workdir / "factory.toml").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return workdir / "src"
    return workdir / contract.source


def _module_file(root: Path, parts: Sequence[str]) -> Path | None:
    base = root.joinpath(*parts)
    return next((c for c in (base / "__init__.py", base.with_suffix(".py")) if c.is_file()), None)


def _top_level_names(tree: ast.Module) -> tuple[set[str], list[str] | None]:
    """Names a module binds at top level, and its ``__all__`` if it declares a literal one."""
    names: set[str] = set()
    dunder_all: list[str] | None = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            names.update(a.asname or a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            names.update(targets)
            if "__all__" in targets and isinstance(node.value, ast.List | ast.Tuple):
                dunder_all = [
                    e.value for e in node.value.elts if isinstance(e, ast.Constant) and e.value
                ]
    return names, dunder_all


def _check_export(workdir: Path, dotted: str) -> str | None:
    """``None`` when ``dotted`` (e.g. ``calc.average``) is importable from the target package.

    Checked by parsing the module, not by importing it: ``check`` runs on the orchestrator, and
    agent-authored code only ever executes inside the sandbox. "Importable" here means the module
    binds the name at top level and, when it declares ``__all__``, lists it there.
    """
    if "." not in dotted:
        return f"export {dotted!r}: must be dotted, e.g. calc.{dotted}"
    *module, symbol = dotted.split(".")
    path = _module_file(_source_root(workdir), module)
    if path is None:
        return f"export {dotted}: no module {'.'.join(module)} under {_source_root(workdir).name}/"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as e:
        return f"export {dotted}: cannot parse {path.name}: {e}"
    names, dunder_all = _top_level_names(tree)
    where = f"{'.'.join(module)}/{path.name}" if path.name == "__init__.py" else path.name
    if symbol not in names:
        return f"export {dotted}: {symbol!r} is not defined or imported in {where}"
    if dunder_all is not None and symbol not in dunder_all:
        return f"export {dotted}: {symbol!r} is missing from __all__ in {where}"
    return None


# ---------------------------------------------------------------- scoring and the baseline


def score(results: Sequence[EvalOutcome]) -> dict[str, Any]:
    """Suite score: ``passed``/``total`` plus the per-eval detail, in suite order.

    This dict IS the baseline format: ``--update-baseline`` writes exactly what a run produced.
    """
    return {
        "passed": sum(1 for r in results if r.passed),
        "total": len(results),
        "evals": {
            r.id: {"slug": r.slug, "passed": r.passed, "failures": r.failures} for r in results
        },
    }


def baseline_diff(current: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Regressions only: evals the baseline records as passing that no longer pass.

    A NEW pass is never a failure (that is the point of an eval suite), and neither is a new
    eval that fails — it is a gap you commit deliberately by updating the baseline. Losing an
    eval the baseline expected IS a regression: the expectation stopped being checked.
    """
    was = baseline.get("evals") or {}
    now = current.get("evals") or {}
    out: list[str] = []
    for eid, before in was.items():
        if not (before or {}).get("passed"):
            continue
        after = now.get(eid)
        if after is None:
            out.append(f"{eid}: passed in the baseline but is no longer in the suite")
        elif not after.get("passed"):
            failures = "; ".join(after.get("failures") or ["(no detail)"])
            out.append(f"{eid}: regressed — {failures}")
    return out


def load_baseline(path: str | Path) -> dict[str, Any]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "evals" not in data:
        raise ValueError(f"{path}: not a suite score (no 'evals' key)")
    return data


# ---------------------------------------------------------------- running the suite


def run_one(
    ev: Eval,
    *,
    bp: Blueprint,
    run_root: Path,
    agent: str = "scripted",
    sandbox: str = "local",
    run_id: str | None = None,
) -> EvalOutcome:
    """Walk the whole pipeline for one eval on a private copy of the target, then ``check`` it.

    Same code path as ``swfactory run`` (``ctx_for`` -> ``setup`` -> ``run_pipeline``), with the
    eval's directory as the fixtures dir and both gates auto-approved. ``scm`` is always local:
    an eval must never open a pull request. A ``StageError`` is the eval's failure, not the
    suite's crash — a blown build loop or a missing fixture is exactly what we are measuring.
    """
    run_dir, workdir = run_root / "run", run_root / "work"
    try:
        issue_path = ev.issue_path.resolve().relative_to(FACTORY_ROOT.resolve()).as_posix()
    except ValueError as e:
        raise ValueError(
            f"eval issue must live below the factory asset root: {ev.issue_path}"
        ) from e
    job = bp.jobs({"issues": [issue_path]})[0]  # the blueprint's first target
    cfg = bp.config(
        job,
        run_id=run_id or run_id_for(ev.id),
        agent=agent,
        sandbox=sandbox,
        scm="local",
        approve="auto",
        fixtures_dir=str(ev.fixtures_dir),
        workdir=str(workdir),
    )
    ctx = ctx_for(cfg, blueprint=bp, run_dir=run_dir)
    try:
        setup(ctx)
        report = run_pipeline(ctx, cli_approver)
    except StageError as e:
        return EvalOutcome(id=ev.id, slug=ev.slug, failures=[f"run failed: {e}"])
    (run_dir / "report.json").write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return EvalOutcome(id=ev.id, slug=ev.slug, failures=check(ev, report, workdir))


def run_suite(
    suite_dir: str | Path = SUITE_DIR,
    *,
    agent: str = "scripted",
    sandbox: str = "local",
    blueprint: str = "factory",
    only: Sequence[str] = (),
    work_root: str | Path | None = None,
    echo: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Run every eval of ``suite_dir`` and return its :func:`score`.

    Each eval gets its own run id, run dir and workdir under ``.factory/evals/<suite run>/``, so
    nothing is shared between evals and a re-run never resumes (a resumed stage records
    ``skipped`` and would score a pass it never earned).
    """
    evals = load_suite(suite_dir)
    if only:
        wanted = set(only)
        evals = [e for e in evals if e.id in wanted or e.slug in wanted]
        if not evals:
            raise ValueError(f"no eval matches {sorted(wanted)} in {suite_dir}")
    bp = blueprint_mod.load(blueprint)
    suite_run = uuid.uuid4().hex[:8]
    root = Path(work_root) if work_root is not None else EVAL_RUNS_DIR / suite_run
    results: list[EvalOutcome] = []
    for i, ev in enumerate(evals, 1):
        echo(f"\n=== eval {i}/{len(evals)}: {ev.slug} ({ev.id}) — {ev.title}")
        outcome = run_one(
            ev,
            bp=bp,
            run_root=root / ev.slug,
            agent=agent,
            sandbox=sandbox,
            run_id=run_id_for(f"{suite_run}:{ev.id}"),
        )
        results.append(outcome)
        echo(f"--- {ev.slug}: {'PASS' if outcome.passed else 'FAIL'}")
        for failure in outcome.failures:
            echo(f"      {failure}")
    return score(results)


def score_table(current: dict[str, Any]) -> str:
    """The suite score as a table (the last thing a human reads in the CI log)."""
    rows = [
        (detail.get("slug") or eid, "pass" if detail.get("passed") else "FAIL", eid)
        for eid, detail in (current.get("evals") or {}).items()
    ]
    width = max((len(r[0]) for r in rows), default=4)
    lines = [f"{slug.ljust(width)}  {verdict:<4}  {eid}" for slug, verdict, eid in rows]
    lines.append(f"{'passed'.ljust(width)}  {current.get('passed')}/{current.get('total')}")
    return "\n".join(lines)


# ---------------------------------------------------------------- python -m swfactory.evals


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m swfactory.evals``: run the suite, gate on the committed baseline.

    Exit 1 on a regression (or an unusable baseline), 0 otherwise. Kept argparse-thin so a typer
    command in ``cli.py`` is a two-line wrapper around ``run_suite``.
    """
    p = argparse.ArgumentParser(prog="swfactory-evals", description=run_suite.__doc__)
    p.add_argument("--suite", default=SUITE_DIR, help=f"eval suite dir (default {SUITE_DIR})")
    p.add_argument("--agent", default="scripted", choices=["scripted", "claude"])
    p.add_argument("--sandbox", default="local", choices=["local", "srt", "docker", "islo"])
    p.add_argument("--blueprint", default="factory")
    p.add_argument("--only", action="append", default=[], metavar="ID|SLUG")
    p.add_argument("--baseline", default=None, help=f"default <suite>/{BASELINE_NAME}")
    p.add_argument("--update-baseline", action="store_true", help="write the score, gate nothing")
    p.add_argument("--json", action="store_true", help="print the score as JSON")
    args = p.parse_args(argv)

    try:
        current = run_suite(
            args.suite,
            agent=args.agent,
            sandbox=args.sandbox,
            blueprint=args.blueprint,
            only=args.only,
        )
    except (OSError, ValueError) as e:
        print(f"eval suite error: {e}", file=sys.stderr)
        return 2
    print("\n" + score_table(current))
    if args.json:
        print(json.dumps(current, indent=2))

    baseline_path = Path(args.baseline or Path(args.suite) / BASELINE_NAME)
    if args.update_baseline:
        baseline_path.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
        print(f"\nbaseline written: {baseline_path}")
        return 0
    try:
        baseline = load_baseline(baseline_path)
    except (OSError, ValueError) as e:
        print(f"\nno usable baseline ({e}); re-run with --update-baseline", file=sys.stderr)
        return 1
    if args.only:  # a subset run cannot claim the evals it did not run went missing
        kept = {k: v for k, v in baseline["evals"].items() if k in current["evals"]}
        baseline = {**baseline, "passed": sum(1 for v in kept.values() if v.get("passed"))}
        baseline["evals"], baseline["total"] = kept, len(kept)
    regressions = baseline_diff(current, baseline)
    if regressions:
        print(f"\n{len(regressions)} regression(s) against {baseline_path}:", file=sys.stderr)
        for line in regressions:
            print(f"  {line}", file=sys.stderr)
        return 1
    print(f"\nno regressions against {baseline_path} ({baseline['passed']}/{baseline['total']})")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
