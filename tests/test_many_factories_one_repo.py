"""Two independently operated factory instances working ONE repository.

Inside one factory, concurrency is fenced: durable admission and capacity (#2058), the Cell epoch
(#2041), the single-writer operation journal (#2042), the accepted-inputs pin (#2065). Across
factories none of that applies -- separate Airflows, separate backends, separate state roots, no
shared store by design (#2093 tried a coordination service and was refused: it handed a new
lower-trust credential an unbounded write path).

What two instances DO share is the repository. These tests pin that it is enough.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from swfactory.config import Config
from swfactory.models import StageError
from swfactory.publication_identity import PublicationIdentity, adopts, instance_id, publication_key
from swfactory.scm import LocalGitScm

ISSUE = "DEMO-1"


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout


def _patch(subject: str, body: str, instance: str = "swf-instance-a") -> bytes:
    """A delivery patch shaped like the ones `stages.commit` produces.

    The `Factory-Instance` trailer is what lets the REMOTE answer "is this branch mine" -- `git am`
    preserves trailers, so the answer rides along with the work instead of living in either
    instance's memory. A fixture without it would be testing a commit the factory never makes.
    """
    return (
        f"From 0000000000000000000000000000000000000000 Mon Sep 17 00:00:00 2001\n"
        f"From: swfactory-bot <bot@example.com>\nDate: Mon, 1 Jan 2026 00:00:00 +0000\n"
        f"Subject: [PATCH] {subject}\n\nFactory-Instance: {instance}\n"
        f"---\n demo/target/f.txt | 1 +\n 1 file changed, 1 insertion(+)\n\n"
        f"diff --git a/demo/target/f.txt b/demo/target/f.txt\nnew file mode 100644\n"
        f"index 0000000..0000001\n--- /dev/null\n+++ b/demo/target/f.txt\n"
        f"@@ -0,0 +1 @@\n+{body}\n-- \n2.39.0\n"
    ).encode()


@pytest.fixture
def remote(tmp_path: Path) -> Path:
    """One bare repository, the only thing the two instances share."""
    bare = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(bare)], check=True)
    seed = tmp_path / "seed"
    subprocess.run(["git", "clone", "-q", str(bare), str(seed)], check=True)
    (seed / "README.md").write_text("seed\n", encoding="utf-8")
    for args in (
        ["add", "-A"],
        ["-c", "user.name=s", "-c", "user.email=s@e", "commit", "-qm", "seed"],
        ["push", "-q", "origin", "main"],
    ):
        subprocess.run(["git", *args], cwd=seed, check=True)
    return bare


def test_two_instances_working_one_issue_converge_on_one_branch() -> None:
    """The publish ref is keyed on the WORK, not the run.

    It used to be `factory/<issue>-<run_id>`, and run_id is per run per instance: two instances on
    one issue opened two branches, and because PR reuse keys on the branch, two pull requests. The
    key is `sha256(repo, target, issue)` -- the same inputs `CellIdentity.stable_id` uses, so the
    two halves of the factory cannot disagree about what "the same work" means.
    """
    cfg = Config(issue="demo/issue.md")
    key = publication_key(cfg.repo, cfg.target_dir, ISSUE)
    assert publication_key(cfg.repo, cfg.target_dir, ISSUE) == key  # deterministic
    assert publication_key(cfg.repo, "other/dir", ISSUE) != key  # a different target is other work
    assert publication_key("other/repo", cfg.target_dir, ISSUE) != key
    assert publication_key(cfg.repo, cfg.target_dir, "DEMO-2") != key


def test_the_second_instance_is_refused_by_the_lease_rather_than_overwriting(remote: Path, tmp_path: Path) -> None:
    """The prize failure: instance B replacing a commit instance A's reviewer already read.

    `factory/*` was force-pushed blind. Both instances now push the SAME ref, so a blind force is
    exactly the overwrite. The lease names the sha this instance observed, and git refuses when
    anyone moved it since.
    """
    branch = f"factory/{ISSUE}-{publication_key('o/r', 'demo/target', ISSUE)}"
    key = publication_key("o/r", "demo/target", ISSUE)
    a = LocalGitScm(remote, tmp_path / "run-a")
    b = LocalGitScm(remote, tmp_path / "run-b")
    id_a = PublicationIdentity(key=key, instance="swf-instance-a")
    id_b = PublicationIdentity(key=key, instance="swf-instance-b")

    a.publish(
        branch=branch,
        patch=_patch("from A", "A", instance="swf-instance-a"),
        title="A",
        body="A",
        labels=["factory"],
        identity=id_a,
    )
    landed = _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip()

    # B observed nothing (it never fetched this ref) and now pushes over a ref that exists.
    with pytest.raises(StageError) as caught:
        b.publish(
            branch=branch,
            patch=_patch("from B", "B", instance="swf-instance-b"),
            title="B",
            body="B",
            labels=["factory"],
            identity=id_b,
        )
    assert "not by this one" in str(caught.value) and "swf-instance-a" in str(caught.value)
    assert _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip() == landed, "A's commit was overwritten"


def test_an_instance_adopts_the_pull_request_that_already_exists(tmp_path: Path) -> None:
    """Adoption is by the marker in the body, parsed strictly.

    A body that merely quotes a key -- a log excerpt in a PR description -- must not adopt the
    wrong pull request, and a PR a person opened carries no marker and belongs to nobody.
    """
    key = publication_key("o/r", "demo/target", ISSUE)
    mine = PublicationIdentity(key=key, instance="swf-a", cell_id="cell_x", epoch=3).marker()

    assert adopts(f"body\n\n{mine}\n", key)
    assert not adopts(f"body\n\n{mine}\n", key[:6]), "a prefix must not adopt"
    assert not adopts(f"see key={key} in the log above", key), "prose is not a marker"
    assert not adopts("a pull request a person opened", key)

    other = PublicationIdentity(key=publication_key("o/r", "demo/target", "DEMO-2"), instance="swf-b").marker()
    assert not adopts(f"body\n\n{other}\n", key), "another Cell's PR is not this work"


def test_an_instance_keeps_its_name_across_restarts(tmp_path: Path) -> None:
    """Derived from the state the instance owns, not from the hostname: two instances legitimately
    share a host, one legitimately moves between them, and a container's hostname changes on every
    restart."""
    root = tmp_path / "state"
    first = instance_id(root)
    assert first and instance_id(root) == first, "an instance must keep its name across restarts"
    assert instance_id(tmp_path / "other-state") != first, "two state roots are two instances"


def test_the_marker_names_the_cell_and_epoch_the_work_belongs_to() -> None:
    """So a reader of the pull request can tell which factory produced it, and against which epoch
    -- the thing that decides who is allowed to publish at all (#2041)."""
    from swfactory.publication_identity import read_marker

    key = publication_key("o/r", "demo/target", ISSUE)
    parsed = read_marker(PublicationIdentity(key=key, instance="swf-a", cell_id="cell_x", epoch=7).marker())
    assert parsed == {"key": key, "instance": "swf-a", "cell": "cell_x", "epoch": "7"}


def test_the_same_instance_may_still_republish_its_own_branch(remote: Path, tmp_path: Path) -> None:
    """The mirror. A retried deliver rebuilds the branch -- `git am` restamps committer dates, so
    the new commits are not descendants of the old -- and that legitimate force must keep working,
    or the refusal above would have broken every retry in the single-instance case."""
    branch = f"factory/{ISSUE}-{publication_key('o/r', 'demo/target', ISSUE)}"
    scm = LocalGitScm(remote, tmp_path / "run-a")
    ident = PublicationIdentity(key=publication_key("o/r", "demo/target", ISSUE), instance="swf-instance-a")

    scm.publish(
        branch=branch,
        patch=_patch("first", "1", instance="swf-instance-a"),
        title="T",
        body="B",
        labels=["factory"],
        identity=ident,
    )
    first = _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip()
    scm.publish(
        branch=branch,
        patch=_patch("second", "2", instance="swf-instance-a"),
        title="T",
        body="B",
        labels=["factory"],
        identity=ident,
    )
    assert _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip() != first, "a retry must re-publish"


def test_the_remote_answers_whose_branch_it_is_across_a_restart(remote: Path, tmp_path: Path) -> None:
    """The reason this lives in a commit trailer and not in memory.

    The first version kept a process-global dict of what this process had pushed. That was three
    wrong things at once: module-level mutable state, a second record of something the commit
    already says, and -- the one that mattered -- an answer that vanished on restart, so a fresh
    process could not tell its own branch from another instance's and refused its own retry.

    Two `LocalGitScm` objects with no shared memory stand in for a restart: the second one has never
    pushed anything, and must still recognise the branch as its own.
    """
    key = publication_key("o/r", "demo/target", ISSUE)
    branch = f"factory/{ISSUE}-{key}"
    ident = PublicationIdentity(key=key, instance="swf-instance-a")

    before = LocalGitScm(remote, tmp_path / "run-before")
    before.publish(
        branch=branch,
        patch=_patch("first", "1", instance="swf-instance-a"),
        title="T",
        body="B",
        labels=["factory"],
        identity=ident,
    )
    first = _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip()

    # A different object, a different run directory: nothing carried over but the remote.
    after = LocalGitScm(remote, tmp_path / "run-after")
    after.publish(
        branch=branch,
        patch=_patch("second", "2", instance="swf-instance-a"),
        title="T",
        body="B",
        labels=["factory"],
        identity=ident,
    )
    assert _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip() != first, (
        "an instance must still recognise its own branch after a restart"
    )


def test_a_branch_nobody_claimed_is_not_ours_to_replace(remote: Path, tmp_path: Path) -> None:
    """A commit with no `Factory-Instance` trailer was published by an older build or by a person.

    We cannot name its owner, so it is not ours to overwrite. This costs nothing real: the branch
    name itself changed with the publication key, so a pre-upgrade ref has a different name and
    never collides with this one -- the only commits that can appear on THIS ref come from code
    that stamps the trailer. What the rule actually protects is a human who pushed here by hand.
    """
    key = publication_key("o/r", "demo/target", ISSUE)
    branch = f"factory/{ISSUE}-{key}"

    # A patch with no trailer: `_patch` always adds one, so build the unclaimed case explicitly.
    unclaimed = _patch("by hand", "H", instance="x").replace(b"Factory-Instance: x\n", b"")
    assert b"Factory-Instance" not in unclaimed

    LocalGitScm(remote, tmp_path / "run-hand").publish(
        branch=branch,
        patch=unclaimed,
        title="H",
        body="H",
        labels=["factory"],
        identity=PublicationIdentity(key=key, instance="swf-instance-a"),
    )
    landed = _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip()

    with pytest.raises(StageError) as caught:
        LocalGitScm(remote, tmp_path / "run-b").publish(
            branch=branch,
            patch=_patch("mine", "M", instance="swf-instance-b"),
            title="M",
            body="M",
            labels=["factory"],
            identity=PublicationIdentity(key=key, instance="swf-instance-b"),
        )
    assert "left no Factory-Instance trailer" in str(caught.value)
    assert _git("rev-parse", f"refs/heads/{branch}", cwd=remote).strip() == landed
