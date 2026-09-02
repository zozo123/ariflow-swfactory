"""LocalGitScm against real git in tmp_path; GitHubScm argv construction with a fake _run."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swfactory import scm as scm_mod
from swfactory.models import Issue, StageError
from swfactory.scm import GitHubScm, LocalGitScm, Scm, parse_issue_file

ROOT = Path(__file__).resolve().parents[1]
IDENT = ["-c", "user.name=tester", "-c", "user.email=tester@example.com"]


@pytest.fixture(autouse=True)
def _isolated_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No user/system git config (signing, hooks templates, credential helpers)."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")


def git(*args: str, cwd: Path, input: bytes | None = None) -> str:
    proc = subprocess.run(
        ["git", *IDENT, *args], cwd=cwd, input=input, capture_output=True, check=True
    )
    return proc.stdout.decode()


def make_source_repo(root: Path) -> tuple[Path, bytes]:
    """Repo with one commit on main plus a bot commit with trailers on a branch -> (repo, patch)."""
    repo = root / "work"
    repo.mkdir()
    git("init", "-q", "-b", "main", cwd=repo)
    (repo / "calc.py").write_text("def add(a, b):\n    return a + b\n")
    git("add", ".", cwd=repo)
    git("commit", "-q", "-m", "baseline", cwd=repo)
    git("checkout", "-q", "-b", "factory/DEMO-1-abc12345", cwd=repo)
    (repo / "calc.py").write_text(
        "def add(a, b):\n    return a + b\n\n\ndef sub(a, b):\n    return a - b\n"
    )
    git(
        "-c", "user.name=swfactory-bot", "-c", "user.email=swfactory-bot@users.noreply.github.com",
        "commit", "-q", "-a", "-m", "build: add sub",
        "--trailer", "Factory-Run=abc12345", "--trailer", "Agent=scripted",
        cwd=repo,
    )  # fmt: skip
    patch = subprocess.run(
        ["git", "format-patch", "--stdout", "main..HEAD"], cwd=repo, capture_output=True, check=True
    ).stdout
    assert patch
    return repo, patch


def test_local_publish_pushes_branch_with_trailers(tmp_path: Path, capsys) -> None:
    source, patch = make_source_repo(tmp_path)
    run_dir = tmp_path / "run"
    scm = LocalGitScm(run_dir / "remote.git", run_dir, base_repo=source)
    assert isinstance(scm, Scm)

    url = scm.publish(
        branch="factory/DEMO-1-abc12345",
        patch=patch,
        title="DEMO-1: add sub",
        body="SCRIPTED REPLAY\n\nadds sub()",
        labels=["factory", "agent-authored"],
    )

    remote = run_dir / "remote.git"
    assert url == f"file://{(run_dir / 'pr.md').resolve()}"
    heads = git("show-ref", "--heads", cwd=remote)
    assert "refs/heads/main" in heads and "refs/heads/factory/DEMO-1-abc12345" in heads
    msg = git("log", "-1", "--format=%an%n%B", "factory/DEMO-1-abc12345", cwd=remote)
    assert msg.startswith("swfactory-bot\n")
    assert "Factory-Run: abc12345" in msg and "Agent: scripted" in msg
    # branch is exactly baseline + one bot commit, on the same base as the source repo
    assert (
        git("rev-parse", "main", cwd=remote).strip() == git("rev-parse", "main", cwd=source).strip()
    )
    assert git("rev-list", "--count", "factory/DEMO-1-abc12345", cwd=remote).strip() == "2"
    pr = (run_dir / "pr.md").read_text()
    assert "# DEMO-1: add sub" in pr and "factory, agent-authored" in pr and "adds sub()" in pr
    assert "DEMO-1: add sub" in capsys.readouterr().out


def test_local_publish_is_idempotent_on_remote_creation(tmp_path: Path) -> None:
    source, patch = make_source_repo(tmp_path)
    run_dir = tmp_path / "run"
    scm = LocalGitScm(run_dir / "remote.git", run_dir, base_repo=source)
    scm.publish(branch="b1", patch=patch, title="t", body="b", labels=[])
    scm.publish(branch="b2", patch=patch, title="t", body="b", labels=[])  # remote already seeded
    heads = git("show-ref", "--heads", cwd=run_dir / "remote.git")
    assert "refs/heads/b1" in heads and "refs/heads/b2" in heads


def test_local_publish_without_base_repo_seeds_orphan_main(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    scm = LocalGitScm(run_dir / "remote.git", run_dir)
    # a patch adding a brand-new file applies cleanly on the empty seed commit
    repo, _ = make_source_repo(tmp_path)
    (repo / "new.txt").write_text("hi\n")
    git("add", "new.txt", cwd=repo)
    git("commit", "-q", "-m", "add new.txt", cwd=repo)
    patch = subprocess.run(
        ["git", "format-patch", "--stdout", "-1"], cwd=repo, capture_output=True, check=True
    ).stdout
    scm.publish(branch="feat", patch=patch, title="t", body="b", labels=["x"])
    remote = run_dir / "remote.git"
    assert git("rev-list", "--count", "main", cwd=remote).strip() == "1"
    assert "new.txt" in git("ls-tree", "--name-only", "feat", cwd=remote)


def test_local_publish_rejects_empty_patch(tmp_path: Path) -> None:
    scm = LocalGitScm(tmp_path / "remote.git", tmp_path)
    with pytest.raises(StageError) as ei:
        scm.publish(branch="b", patch=b"", title="t", body="b", labels=[])
    assert ei.value.kind == "scm"


def test_parse_demo_issue() -> None:
    issue = parse_issue_file(ROOT / "demo" / "issue.md")
    assert issue.id == "DEMO-1"
    assert issue.title == "Add percent_change(old, new) to calc"
    assert issue.labels == ["factory"]
    assert "percent_change" in issue.body
    assert issue.body.startswith("As a user of `calc`")
    assert issue.url and issue.url.startswith("file://")


def test_local_fetch_issue(tmp_path: Path) -> None:
    scm = LocalGitScm(tmp_path / "remote.git", tmp_path)
    with pytest.raises(StageError, match="cannot fetch GitHub issues"):
        scm.fetch_issue("42")
    assert scm.fetch_issue(str(ROOT / "demo" / "issue.md")).id == "DEMO-1"
    with pytest.raises(StageError):
        scm.fetch_issue(str(tmp_path / "missing.md"))


def test_local_open_issue_roundtrips(tmp_path: Path) -> None:
    scm = LocalGitScm(tmp_path / "remote.git", tmp_path)
    url = scm.open_issue(title="Cycle time up 3σ", body="mean 10 -> 40", labels=["factory"])
    path = Path(url.removeprefix("file://"))
    assert path.name == "issue-cycle-time-up-3.md"
    issue = scm.fetch_issue(str(path))
    assert issue.title == "Cycle time up 3σ"
    assert issue.labels == ["factory"]
    assert issue.body.strip() == "mean 10 -> 40"


def test_make_scm_local(tmp_path: Path) -> None:
    from swfactory.config import Config

    cfg = Config(issue="demo/issue.md")
    scm = scm_mod.make_scm(cfg, tmp_path, base_repo=tmp_path / "work")
    assert isinstance(scm, LocalGitScm)
    assert scm.remote_dir == tmp_path / "remote.git"
    assert scm.base_repo == tmp_path / "work"
    gh = scm_mod.make_scm(Config(issue="1", scm="github", repo="o/r", base_branch="dev"), tmp_path)
    assert isinstance(gh, GitHubScm) and gh.repo == "o/r" and gh.base_branch == "dev"


# ---------------------------------------------------------------- GitHubScm (fake _run)


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    recorded: list[list[str]] = []

    def fake_run(argv, cwd, input=None):
        recorded.append(list(argv))
        if argv[:3] == ["gh", "pr", "create"]:
            return "https://github.com/o/r/pull/7\n"
        if argv[:3] == ["gh", "issue", "create"]:
            return "\nhttps://github.com/o/r/issues/9\n"
        if argv[:3] == ["gh", "issue", "view"]:
            return (
                '{"number": 42, "title": "T", "body": "B", '
                '"labels": [{"name": "factory"}], "url": "https://github.com/o/r/issues/42"}'
            )
        return ""

    monkeypatch.setattr(scm_mod, "_run", fake_run)
    monkeypatch.setenv("GH_TOKEN", "dummy")
    return recorded


def test_github_publish_argv(calls: list[list[str]]) -> None:
    scm = GitHubScm("o/r", "main")
    assert isinstance(scm, Scm)
    url = scm.publish(
        branch="factory/42-abc", patch=b"From x\n", title="T", body="B",
        labels=["factory", "agent-authored", "factory:blocked"],
    )  # fmt: skip
    assert url == "https://github.com/o/r/pull/7"
    joined = [" ".join(c) for c in calls]
    assert not any("merge" in c for c in joined)
    assert not any("dummy" in c for c in joined)  # token never in argv
    clone = next(c for c in calls if "clone" in c)
    assert clone[:1] == ["git"] and "--depth" in clone and "50" in clone
    assert clone[clone.index("--branch") + 1] == "main"
    assert "https://github.com/o/r.git" in clone
    assert any(c[0] == "git" and c[-2:] == ["checkout", "-b", "factory/42-abc"][-2:] for c in calls)
    assert any("am" in c and "--3way" in c for c in calls)
    assert any(c[0] == "git" and c[1:] == ["push", "-u", "origin", "factory/42-abc"] for c in calls)
    label_creates = [c for c in calls if c[:3] == ["gh", "label", "create"]]
    assert [c[3] for c in label_creates] == ["factory", "agent-authored", "factory:blocked"]
    assert all("--force" in c for c in label_creates)
    pr = next(c for c in calls if c[:3] == ["gh", "pr", "create"])
    assert pr[pr.index("--repo") + 1] == "o/r"
    assert pr[pr.index("--base") + 1] == "main"
    assert pr[pr.index("--head") + 1] == "factory/42-abc"
    assert pr[pr.index("--title") + 1] == "T"
    assert "--body-file" in pr
    labels = [pr[i + 1] for i, tok in enumerate(pr) if tok == "--label"]
    assert labels == ["factory", "agent-authored", "factory:blocked"]
    assert calls.index(pr) > calls.index(label_creates[-1])  # labels exist before the PR


def test_github_publish_requires_token(calls, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GH_TOKEN")
    with pytest.raises(StageError, match="GH_TOKEN"):
        GitHubScm("o/r", "main").publish(branch="b", patch=b"x", title="t", body="b", labels=[])
    assert calls == []


def test_github_fetch_issue(calls: list[list[str]]) -> None:
    issue = GitHubScm("o/r", "main").fetch_issue("42")
    assert issue == Issue(
        id="42", title="T", body="B", labels=["factory"], url="https://github.com/o/r/issues/42"
    )
    view = calls[0]
    assert view[:5] == ["gh", "issue", "view", "42", "--repo"] and view[5] == "o/r"
    assert view[view.index("--json") + 1] == "number,title,body,labels,url"
    assert GitHubScm("o/r", "main").fetch_issue(str(ROOT / "demo" / "issue.md")).id == "DEMO-1"


def test_github_open_issue_argv(calls: list[list[str]]) -> None:
    url = GitHubScm("o/r", "main").open_issue(title="T", body="B", labels=["factory"])
    assert url == "https://github.com/o/r/issues/9"
    create = next(c for c in calls if c[:3] == ["gh", "issue", "create"])
    assert create[create.index("--repo") + 1] == "o/r"
    assert create[create.index("--title") + 1] == "T"
    assert "--body-file" in create and create[create.index("--label") + 1] == "factory"
    assert not any("merge" in " ".join(c) for c in calls)


def test_scm_protocol_has_no_merge() -> None:
    assert not hasattr(Scm, "merge")
    assert not hasattr(LocalGitScm, "merge") and not hasattr(GitHubScm, "merge")
