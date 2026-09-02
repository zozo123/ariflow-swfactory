"""Source-control adapters. Run ONLY on the orchestrator, which holds the GitHub credential.

The sandbox never pushes: ``deliver`` extracts the bot-authored commits as a ``git format-patch``
stream and hands the bytes to :meth:`Scm.publish`, which applies them on a fresh clone of the base
branch and opens a pull request. There is deliberately **no merge method** on the protocol: the PR
plus branch protection/CODEOWNERS is the release gate.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

import yaml

from swfactory.config import Config
from swfactory.models import Issue, StageError

BOT_NAME = "swfactory-bot"
BOT_EMAIL = "swfactory-bot@users.noreply.github.com"
# Committer identity for `git am` so a bare CI/orchestrator host needs no git config.
_GIT_IDENT = ["-c", f"user.name={BOT_NAME}", "-c", f"user.email={BOT_EMAIL}"]
_FRONT_MATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---\r?\n?(.*)\Z", re.DOTALL)


@runtime_checkable
class Scm(Protocol):
    """Runs ONLY on the orchestrator. Holds the GitHub credential. Has no merge method by design."""

    kind: Literal["local", "github"]

    def fetch_issue(self, ref: str) -> Issue:
        """Numeric ref -> GitHub issue; path -> front-matter markdown file."""
        ...

    def publish(
        self, *, branch: str, patch: bytes, title: str, body: str, labels: Sequence[str]
    ) -> str:
        """Apply ``patch`` (format-patch bytes) on a fresh clone, push ``branch``, open a PR.

        Returns the PR url (GitHub) or a ``file://`` url of the printed PR markdown (local).
        """
        ...

    def open_issue(self, *, title: str, body: str, labels: Sequence[str]) -> str:
        """Open a new issue (used by maintain's 3-sigma tier). Returns its url."""
        ...


# ---------------------------------------------------------------- shared helpers


def _run(argv: Sequence[str], cwd: Path | None, input: bytes | None = None) -> str:
    """Run one subprocess and return stdout; non-zero exit -> StageError("scm", retryable=True)."""
    try:
        proc = subprocess.run(
            list(argv), cwd=cwd, input=input, capture_output=True, check=False, timeout=600
        )
    except FileNotFoundError as e:
        raise StageError("scm", f"{argv[0]} not found on PATH", retryable=False) from e
    except subprocess.TimeoutExpired as e:
        raise StageError("scm", f"timed out: {' '.join(argv)}", retryable=True) from e
    stdout = proc.stdout.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise StageError(
            "scm", f"{' '.join(argv)} failed (rc={proc.returncode}): {stderr}", retryable=True
        )
    return stdout


def parse_issue_file(path: Path) -> Issue:
    """Parse a ``--- yaml ---`` front-matter markdown file into an Issue. Body is kept verbatim."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise StageError("scm", f"cannot read issue file {path}: {e}") from e
    m = _FRONT_MATTER.match(text)
    if not m:
        raise StageError("scm", f"{path} has no '---' front matter")
    try:
        meta = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError as e:
        raise StageError("scm", f"{path}: invalid front matter: {e}") from e
    if not isinstance(meta, dict) or "id" not in meta or "title" not in meta:
        raise StageError("scm", f"{path}: front matter needs 'id' and 'title'")
    labels = meta.get("labels") or []
    if isinstance(labels, str):
        labels = [labels]
    return Issue(
        id=str(meta["id"]),
        title=str(meta["title"]),
        body=m.group(2),
        labels=[str(label) for label in labels],
        url=path.resolve().as_uri(),
    )


def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-").lower()[:60] or "untitled"


def _pr_markdown(title: str, labels: Sequence[str], body: str) -> str:
    return f"# {title}\n\nlabels: {', '.join(labels) or '(none)'}\n\n{body.rstrip()}\n"


def _apply_and_push(clone: Path, *, branch: str, patch: bytes) -> None:
    """checkout -b, `git am --3way` the patch (keeps bot author + trailers), push -u."""
    if not patch.strip():
        raise StageError("scm", "empty patch: nothing to publish")
    _run(["git", "checkout", "-b", branch], clone)
    _run(["git", *_GIT_IDENT, "am", "--3way"], clone, input=patch)
    _run(["git", "push", "-u", "origin", branch], clone)


# ---------------------------------------------------------------- local (demo / CI)


class LocalGitScm:
    """Bare git repo standing in for GitHub; the "PR" is a markdown file printed to stdout.

    ``base_repo``/``base_ref``: when given, the bare remote is seeded by pushing that ref of that
    repo as ``main``. The factory passes the sandbox workdir and its recorded baseline (a branch
    name or the sha stored at ``.factory/base``) so the format-patch stream applies on the very
    history it was produced from. Without it an orphan ``main`` with one empty commit is created,
    on which a patch whose first commit adds pre-existing files will not apply.
    """

    kind: Literal["local", "github"] = "local"

    def __init__(
        self,
        remote_dir: Path,
        run_dir: Path,
        base_repo: Path | None = None,
        base_ref: str = "main",
    ) -> None:
        # Absolute: the seeding push runs with cwd=base_repo, where a relative remote_dir would
        # not resolve.
        self.remote_dir = Path(remote_dir).resolve()
        self.run_dir = Path(run_dir).resolve()
        self.base_repo = Path(base_repo).resolve() if base_repo is not None else None
        self.base_ref = base_ref

    def fetch_issue(self, ref: str) -> Issue:
        """Only front-matter files are supported locally."""
        if ref.strip().isdigit():
            raise StageError("scm", "local scm cannot fetch GitHub issues")
        return parse_issue_file(Path(ref))

    def publish(
        self, *, branch: str, patch: bytes, title: str, body: str, labels: Sequence[str]
    ) -> str:
        """Push ``branch`` into the bare remote and write/print ``run_dir/pr.md``."""
        if not patch.strip():
            raise StageError("scm", "empty patch: nothing to publish")
        self._ensure_remote()
        with tempfile.TemporaryDirectory(prefix="swf-clone-") as tmp:
            clone = Path(tmp) / "clone"
            _run(["git", "clone", "--quiet", str(self.remote_dir), str(clone)], None)
            _apply_and_push(clone, branch=branch, patch=patch)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        pr = self.run_dir / "pr.md"
        text = _pr_markdown(title, labels, body)
        pr.write_text(text, encoding="utf-8")
        print(text)
        return f"file://{pr.resolve()}"

    def open_issue(self, *, title: str, body: str, labels: Sequence[str]) -> str:
        """Write ``run_dir/issue-<slug>.md`` (front matter + body) and return its file:// url."""
        self.run_dir.mkdir(parents=True, exist_ok=True)
        slug = _slug(title)
        path = self.run_dir / f"issue-{slug}.md"
        meta = yaml.safe_dump(
            {"id": slug, "title": title, "labels": list(labels)}, sort_keys=False
        ).rstrip()
        path.write_text(f"---\n{meta}\n---\n{body.rstrip()}\n", encoding="utf-8")
        return f"file://{path.resolve()}"

    # -- internals

    def _ensure_remote(self) -> None:
        if not (self.remote_dir / "HEAD").exists():
            self.remote_dir.parent.mkdir(parents=True, exist_ok=True)
            _run(["git", "init", "--quiet", "--bare", str(self.remote_dir)], None)
        _run(["git", "symbolic-ref", "HEAD", "refs/heads/main"], self.remote_dir)
        if self._has_refs():
            return
        if self.base_repo is not None:
            _run(
                [
                    "git",
                    "push",
                    "--quiet",
                    str(self.remote_dir),
                    f"{self.base_ref}:refs/heads/main",
                ],
                self.base_repo,
            )
            return
        with tempfile.TemporaryDirectory(prefix="swf-seed-") as tmp:
            seed = Path(tmp) / "seed"
            _run(["git", "clone", "--quiet", str(self.remote_dir), str(seed)], None)
            _run(["git", "symbolic-ref", "HEAD", "refs/heads/main"], seed)
            _run(["git", *_GIT_IDENT, "commit", "--allow-empty", "-q", "-m", "factory: seed"], seed)
            _run(["git", "push", "--quiet", "origin", "main"], seed)

    def _has_refs(self) -> bool:
        # `git show-ref` exits 1 on an empty repo; that is not an error here.
        proc = subprocess.run(
            ["git", "show-ref", "--heads"], cwd=self.remote_dir, capture_output=True, check=False
        )
        return proc.returncode == 0


# ---------------------------------------------------------------- GitHub (real path)


class GitHubScm:
    """GitHub via ``gh`` and ``git``. Needs the bot token in ``$<token_env>`` on the orchestrator.

    ``gh`` reads ``GH_TOKEN``/``GITHUB_TOKEN`` itself; git pushes use an inline credential helper
    that echoes ``$<token_env>``, so the token never appears in argv or in a clone URL.
    """

    kind: Literal["local", "github"] = "github"

    def __init__(self, repo: str, base_branch: str, token_env: str = "GH_TOKEN") -> None:
        self.repo = repo
        self.base_branch = base_branch
        self.token_env = token_env

    def fetch_issue(self, ref: str) -> Issue:
        """Numeric -> ``gh issue view``; anything else -> front-matter file."""
        if not ref.strip().isdigit():
            return parse_issue_file(Path(ref))
        out = _run(
            [
                "gh", "issue", "view", ref.strip(), "--repo", self.repo,
                "--json", "number,title,body,labels,url",
            ],
            None,
        )  # fmt: skip
        try:
            data = json.loads(out)
        except json.JSONDecodeError as e:
            raise StageError("scm", f"gh issue view returned non-JSON: {out[:200]}") from e
        return Issue(
            id=str(data["number"]),
            title=data["title"],
            body=data.get("body") or "",
            labels=[label["name"] for label in data.get("labels") or []],
            url=data.get("url"),
        )

    def publish(
        self, *, branch: str, patch: bytes, title: str, body: str, labels: Sequence[str]
    ) -> str:
        """Shallow clone of base -> checkout -b -> git am --3way -> push -u -> gh pr create."""
        if not patch.strip():
            raise StageError("scm", "empty patch: nothing to publish")
        self._require_token()
        with tempfile.TemporaryDirectory(prefix="swf-clone-") as tmp:
            clone = Path(tmp) / "clone"
            _run(
                [
                    "git", *self._cred, "clone", "--quiet", "--depth", "50",
                    "--branch", self.base_branch, f"https://github.com/{self.repo}.git", str(clone),
                ],
                None,
            )  # fmt: skip
            # Persist the helper in the clone so `git push` uses it (empty value resets globals).
            _run(["git", "config", "--add", "credential.helper", ""], clone)
            _run(["git", "config", "--add", "credential.helper", self._helper], clone)
            _apply_and_push(clone, branch=branch, patch=patch)
            self._ensure_labels(labels)
            body_file = Path(tmp) / "pr-body.md"
            body_file.write_text(body, encoding="utf-8")
            out = _run(
                [
                    "gh", "pr", "create", "--repo", self.repo, "--base", self.base_branch,
                    "--head", branch, "--title", title, "--body-file", str(body_file),
                    *_label_flags(labels),
                ],
                None,
            )  # fmt: skip
        return _last_line(out)

    def open_issue(self, *, title: str, body: str, labels: Sequence[str]) -> str:
        """``gh issue create``; returns the issue url."""
        self._ensure_labels(labels)
        with tempfile.TemporaryDirectory(prefix="swf-issue-") as tmp:
            body_file = Path(tmp) / "issue-body.md"
            body_file.write_text(body, encoding="utf-8")
            out = _run(
                [
                    "gh", "issue", "create", "--repo", self.repo, "--title", title,
                    "--body-file", str(body_file), *_label_flags(labels),
                ],
                None,
            )  # fmt: skip
        return _last_line(out)

    # -- internals

    @property
    def _helper(self) -> str:
        return f'!f() {{ echo username={BOT_NAME}; echo "password=${{{self.token_env}}}"; }}; f'

    @property
    def _cred(self) -> list[str]:
        return ["-c", "credential.helper=", "-c", f"credential.helper={self._helper}"]

    def _require_token(self) -> None:
        if not os.environ.get(self.token_env):
            raise StageError("scm", f"{self.token_env} is not set; cannot push to GitHub")

    def _ensure_labels(self, labels: Sequence[str]) -> None:
        for label in labels:
            _run(["gh", "label", "create", label, "--repo", self.repo, "--force"], None)


def _label_flags(labels: Sequence[str]) -> list[str]:
    flags: list[str] = []
    for label in labels:
        flags += ["--label", label]
    return flags


def _last_line(out: str) -> str:
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if not lines:
        raise StageError("scm", "gh returned no url")
    return lines[-1]


# ---------------------------------------------------------------- factory


def make_scm(
    cfg: Config, run_dir: Path, base_repo: Path | None = None, base_ref: str = "main"
) -> Scm:
    """Build the Scm named by ``cfg.scm``. ``base_repo``/``base_ref`` only matter for local."""
    if cfg.scm == "github":
        return GitHubScm(cfg.repo, cfg.base_branch)
    return LocalGitScm(Path(run_dir) / "remote.git", Path(run_dir), base_repo, base_ref)
