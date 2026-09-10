"""Source-control adapters. Run ONLY on the orchestrator, which holds the GitHub credential.

The sandbox never pushes: ``deliver`` extracts the bot-authored commits as a ``git format-patch``
stream and hands the bytes to :meth:`Scm.publish`, which applies them on a fresh clone of the base
branch and opens a pull request. There is deliberately **no merge method** on the protocol: the PR
plus branch protection/CODEOWNERS is the release gate.
"""

from __future__ import annotations

import hashlib
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
from swfactory.publication_identity import PublicationIdentity, adopts
from swfactory.recovery_accounting import PublicationReceipt

BOT_NAME = "swfactory-bot"
BOT_EMAIL = "swfactory-bot@users.noreply.github.com"
FACTORY_BRANCH_PREFIX = "factory/"  # bot-owned refs: re-publishing a run force-updates them
# Committer identity for `git am` so a bare CI/orchestrator host needs no git config.
_GIT_IDENT = ["-c", f"user.name={BOT_NAME}", "-c", f"user.email={BOT_EMAIL}"]

# Git forks `git maintenance run --auto --detach` after am/commit/fetch/push. That background
# process keeps writing into .git after the foreground command has already returned, so a clone in
# a TemporaryDirectory can still be growing while cleanup walks it -> ENOTEMPTY. Every clone here is
# disposable and pushed immediately; repacking it is pure waste, and a detached child that outlives
# the directory it writes to is also a process we cannot account for on the way out of a sandbox.
_GIT_NO_AUTO_GC = ["-c", "gc.auto=0", "-c", "maintenance.auto=false"]
_FRONT_MATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---\r?\n?(.*)\Z", re.DOTALL)
# `state` is part of the issue: without it the adapter hands intake a closed issue as work.
_ISSUE_FIELDS = "number,title,body,labels,url,state"


@runtime_checkable
class Scm(Protocol):
    """Runs ONLY on the orchestrator. Holds the GitHub credential. Has no merge method by design."""

    kind: Literal["local", "github"]

    def fetch_issue(self, ref: str) -> Issue:
        """Numeric ref -> GitHub issue; path -> front-matter markdown file."""
        ...

    def publish(
        self,
        *,
        branch: str,
        patch: bytes,
        title: str,
        body: str,
        labels: Sequence[str],
        allowed_prefixes: Sequence[str] | None = None,
        identity: PublicationIdentity | None = None,
    ) -> str:
        """Apply ``patch`` (format-patch bytes) on a fresh clone, push ``branch``, open a PR.

        The patch is policy-checked first (:func:`validate_patch`, :func:`scan_secrets`);
        ``allowed_prefixes`` additionally confines every touched path to those directories.
        Returns the PR url (GitHub) or a ``file://`` url of the printed PR markdown (local).
        """
        ...

    def open_issue(self, *, title: str, body: str, labels: Sequence[str]) -> str:
        """Open a new issue (used by maintain's 3-sigma tier). Returns its url."""
        ...


# ---------------------------------------------------------------- shared helpers


def _run(argv: Sequence[str], cwd: Path | None, input: bytes | None = None) -> str:
    """Run one subprocess and return stdout; non-zero exit -> StageError("scm", retryable=True).

    Git invocations are hardened here rather than at each call site so a future ``git`` command
    cannot reintroduce a detached background writer by forgetting the flags.
    """
    argv = list(argv)
    if argv and argv[0] == "git":
        argv = [argv[0], *_GIT_NO_AUTO_GC, *argv[1:]]
    try:
        proc = subprocess.run(argv, cwd=cwd, input=input, capture_output=True, check=False, timeout=600)
    except FileNotFoundError as e:
        raise StageError("scm", f"{argv[0]} not found on PATH", retryable=False) from e
    except subprocess.TimeoutExpired as e:
        raise StageError("scm", f"timed out: {' '.join(argv)}", retryable=True) from e
    stdout = proc.stdout.decode("utf-8", errors="replace")
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise StageError("scm", f"{' '.join(argv)} failed (rc={proc.returncode}): {stderr}", retryable=True)
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
        state=str(meta.get("state") or "open"),
    )


def _issue_from_gh(data: dict) -> Issue:
    return Issue(
        id=str(data["number"]),
        title=data["title"],
        body=data.get("body") or "",
        labels=[label["name"] for label in data.get("labels") or []],
        url=data.get("url"),
        state=str(data.get("state") or "open"),
    )


def _gh_json(argv: Sequence[str]) -> object:
    out = _run(argv, None)
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise StageError("scm", f"{' '.join(argv[:3])} returned non-JSON: {out[:200]}") from e


def _window(rows: list, *, limit: int, what: str) -> list:
    """``rows`` was asked for with ``limit + 1``: a full window means the listing is truncated,
    and a truncated backlog scan must fail loudly, or the issues past the window are silently
    never selected and their order is whatever ``gh`` returned."""
    if len(rows) > limit:
        raise StageError("scm", f"more than {limit} {what}; the scan is truncated -- raise the window or narrow it")
    return rows


def _slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-").lower()[:60] or "untitled"


def _pr_markdown(title: str, labels: Sequence[str], body: str) -> str:
    return f"# {title}\n\nlabels: {', '.join(labels) or '(none)'}\n\n{body.rstrip()}\n"


def _apply_and_push(clone: Path, *, branch: str, patch: bytes) -> None:
    """checkout -b, `git am --3way` the patch (keeps bot author + trailers), push -u.

    ``factory/*`` is the bot-owned namespace: a retry of ``deliver`` rebuilds the same branch
    (``git am`` restamps committer dates, so even an identical patch yields new shas), so those
    refs are force-pushed. Any other branch keeps plain (fast-forward only) push semantics.
    """
    _run(["git", "checkout", "-b", branch], clone)
    _run(["git", *_GIT_IDENT, "am", "--3way"], clone, input=patch)
    if not branch.startswith(FACTORY_BRANCH_PREFIX):
        _run(["git", "push", "-u", "origin", branch], clone)
        return
    # Compare-and-swap against WHAT THIS INSTANCE LAST PUSHED, not against what it just observed.
    # The branch is keyed on the work rather than the run (see `Ctx.branch`), so a second factory
    # instance working the same issue pushes THIS ref. Leasing against the observed sha is not
    # enough: the second instance observes the first one's commit, does not contain it (a fresh
    # `git am` on the base), and force-pushes straight over it with a perfectly valid lease. The
    # question is not "did the ref move since I looked" but "is the thing on the remote mine".
    #
    # `git am` restamps committer dates, so a retry of THIS run produces commits that are not
    # descendants of the ones it pushed before -- which is why a retry legitimately needs a force
    # and cannot be expressed as a fast-forward.
    remote_head = _remote_head(clone, branch)
    if not remote_head:
        _run(["git", "push", "-u", "origin", branch], clone)
        return
    # Both sides of the comparison come from commits: the patch just applied says who made it, the
    # remote head says who made that. No caller has to know its own name, so the managed boundary
    # (which publishes on behalf of a worker in another process) cannot get it wrong either.
    mine = _instance_of(clone, "HEAD")
    holder = _instance_of(clone, remote_head)
    if not mine or holder != mine:
        raise StageError(
            "scm",
            f"{branch} on the remote is at {remote_head[:12]}, published by "
            f"{holder or 'an instance that left no Factory-Instance trailer'}, not by this one "
            f"({mine or 'a patch with no Factory-Instance trailer'}). Another factory session is "
            "working this issue. Refusing to overwrite it; adopt its pull request instead.",
            retryable=False,
        )
    try:
        _run(["git", "push", "-u", f"--force-with-lease={branch}:{remote_head}", "origin", branch], clone)
    except StageError as error:
        if _is_lease_refusal(str(error)):
            raise StageError(
                "scm",
                f"another factory instance moved {branch} while this one was publishing; refusing "
                "to overwrite it. Re-run deliver: it will adopt the existing pull request.",
                retryable=True,
            ) from error
        raise


def _instance_of(clone: Path, sha: str) -> str:
    """The factory instance named by a commit's ``Factory-Instance`` trailer, or "" if it has none.

    The remote is the single source of truth for "is this branch mine". An earlier version kept a
    process-global dict of what this process had pushed, which was three wrong things at once: it
    was module-level mutable state, it did not survive a restart, and it was a second record of
    something the commit already says. `git am` preserves trailers, so the answer rides along with
    the work and any instance can read it.

    A commit with no trailer belongs to nobody this code can name -- an older build, or a person --
    and is therefore not ours to replace.
    """
    try:
        _run(["git", "rev-parse", "--verify", "--quiet", f"{sha}^{{commit}}"], clone)
    except StageError:  # not in the clone yet: the ref moved after we cloned
        _run(["git", "fetch", "--quiet", "--depth", "1", "origin", sha], clone)
    message = _run(["git", "log", "-1", "--format=%(trailers:key=Factory-Instance,valueonly)", sha], clone)
    return message.strip().splitlines()[0].strip() if message.strip() else ""


def _remote_head(clone: Path, branch: str) -> str:
    """The sha the remote currently holds for ``branch``, or "" when it has no such ref."""
    out = _run(["git", "ls-remote", "origin", f"refs/heads/{branch}"], clone)
    first = out.split(maxsplit=1)
    return first[0] if first else ""


def patch_content_digest(patch: bytes) -> str:
    """The content identity of a format-patch stream: sha256 over its ``git patch-id --stable`` ids.

    ``git am`` restamps committer dates, so a re-published identical patch has new commit shas and
    a receipt keyed on shas could never recognise its own retry. patch-id hashes the hunks and
    nothing else, so it is the same for the bytes handed to ``publish`` and for the commits GitHub
    later shows on the branch -- the one comparison that proves "the remote holds THIS change"
    without trusting a PR body anyone with write access can edit.
    """
    out = _run(["git", "patch-id", "--stable"], None, input=patch)
    ids = [line.split()[0] for line in out.splitlines() if line.strip()]
    return hashlib.sha256("\n".join(ids).encode()).hexdigest()


def _is_lease_refusal(message: str) -> bool:
    """Whether a push failure is the lease refusing, rather than a network or auth problem.

    Distinguished because they call for opposite responses: a lease refusal means another writer
    is live and this instance must adopt rather than retry harder, while a transport error is
    worth retrying as-is.
    """
    lowered = message.lower()
    return "stale info" in lowered or "force-with-lease" in lowered or "fetch first" in lowered


# ---------------------------------------------------------------- patch policy (pure)

_DIFF_HEADER = "diff --git "
_RENAME_COPY = re.compile(r"^(?:rename|copy) (?:from|to) (.+)$")
_FILE_LINE = re.compile(r"^(?:---|\+\+\+) (.+?)\t?$")
_SYMLINK_MODE = re.compile(r"^new (?:file )?mode 120000$", re.MULTILINE)
_C_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "a": "\a", "b": "\b", "f": "\f", "v": "\v"}
SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("aws-access-key-id", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("anthropic-api-key", re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}")),
    ("github-token", re.compile(r"ghp_[A-Za-z0-9]{36}")),
    ("github-fine-grained-token", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
    ("private-key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("google-api-key", re.compile(r"AIza[0-9A-Za-z_-]{35}")),
    ("slack-token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
)


def _unquote(s: str) -> str:
    """Undo git's C-style path quoting (``"a/sp\\tace"``)."""
    if not (len(s) >= 2 and s[0] == '"' and s[-1] == '"'):
        return s

    def repl(m: re.Match[str]) -> str:
        return chr(int(m.group(1), 8)) if m.group(1) else _C_ESCAPES.get(m.group(2), m.group(2))

    return re.sub(r"\\(?:([0-7]{3})|(.))", repl, s[1:-1])


def _strip_ab(name: str) -> str:
    """Unquote a header/file-line name and drop git's ``a/``/``b/`` prefix."""
    return re.sub(r"^[ab]/", "", _unquote(name))


def _header_paths(rest: str) -> list[str]:
    """Both names of a ``diff --git a/X b/Y`` header (mirrors git's own ambiguity handling)."""
    if rest.startswith('"') or rest.endswith('"'):
        return [_strip_ab(n) for n in re.findall(r'"(?:[^"\\]|\\.)*"|\S+', rest)]
    # Unquoted names may contain spaces; identical names split exactly in the middle.
    if len(rest) % 2 == 1:
        mid = len(rest) // 2
        a, b = rest[:mid], rest[mid + 1 :]
        if a.startswith("a/") and b.startswith("b/") and a[2:] == b[2:]:
            return [a[2:]]
    a, _, b = rest.partition(" b/")
    return [a.removeprefix("a/"), b]


def patch_paths(patch: bytes) -> list[str]:
    """Every repo-relative path a format-patch stream touches, in order, without duplicates.

    Sources: ``diff --git`` headers plus the ``rename/copy from/to`` and ``---``/``+++`` lines of
    each per-file header (up to its first hunk, so hunk content is never mistaken for a path).
    Over-collecting is fine: the result feeds a deny check, so a spurious path can only reject.
    """
    seen: dict[str, None] = {}
    in_header = False
    for line in patch.decode("utf-8", errors="replace").splitlines():
        if line.startswith(_DIFF_HEADER):
            in_header = True
            for p in _header_paths(line[len(_DIFF_HEADER) :]):
                seen.setdefault(p, None)
            continue
        if not in_header:
            continue
        if line.startswith("@@"):
            in_header = False
        elif m := _RENAME_COPY.match(line):
            seen.setdefault(_unquote(m.group(1)), None)
        elif m := _FILE_LINE.match(line):
            p = _strip_ab(m.group(1))
            if p != "/dev/null":
                seen.setdefault(p, None)
    return list(seen)


def _norm_prefix(prefix: str) -> str:
    return prefix.strip().removeprefix("./").strip("/")


def validate_patch(patch: bytes, *, allowed_prefixes: Sequence[str] | None = None) -> None:
    """Reject a patch that could escape the checkout or smuggle a symlink; ``StageError("policy")``.

    Always: no absolute paths, no ``..`` components, nothing under ``.git/``, no symlink modes.
    With ``allowed_prefixes``: every touched path must lie under one of them (``""``/``"."`` =
    whole repo).
    """
    if _SYMLINK_MODE.search(patch.decode("utf-8", errors="replace")):
        raise StageError("policy", "patch creates a symlink (mode 120000)")
    prefixes = None if allowed_prefixes is None else [_norm_prefix(p) for p in allowed_prefixes]
    for path in patch_paths(patch):
        parts = path.split("/")
        if path.startswith("/") or (len(path) > 1 and path[1] == ":"):
            raise StageError("policy", f"patch touches absolute path: {path}")
        if ".." in parts:
            raise StageError("policy", f"patch path escapes the checkout: {path}")
        if ".git" in parts:
            raise StageError("policy", f"patch touches .git: {path}")
        if prefixes is not None and not any(p == "" or path == p or path.startswith(p + "/") for p in prefixes):
            raise StageError("policy", f"patch touches {path}, outside allowed {sorted(set(prefixes))}")


def scan_secrets(patch: bytes) -> list[str]:
    """Names of the secret shapes found anywhere in ``patch`` (:data:`SECRET_PATTERNS`)."""
    text = patch.decode("utf-8", errors="replace")
    return [kind for kind, rx in SECRET_PATTERNS if rx.search(text)]


def _check_patch(patch: bytes, allowed_prefixes: Sequence[str] | None) -> None:
    """The gate every ``publish`` runs before touching git or the network: an empty stream is
    nothing to publish, and the patch must pass the path policy and the secret scan."""
    if not patch.strip():
        raise StageError("scm", "empty patch: nothing to publish")
    validate_patch(patch, allowed_prefixes=allowed_prefixes)
    if hits := scan_secrets(patch):
        raise StageError("policy", f"secret-like token in patch: {', '.join(hits)}")


# ---------------------------------------------------------------- local (demo / CI)


class LocalGitScm:
    """Bare git repo standing in for GitHub; the "PR" is a markdown file printed to stdout.

    ``base_repo``/``base_ref``: when given, the bare remote is seeded by pushing that ref of that
    repo as ``main``. The factory passes the sandbox workdir and its host-recorded baseline so the
    format-patch stream applies on the very history it was produced from. Without a host workdir
    (islo sandbox) ``seed_url``/``seed_ref``
    seed ``main`` by fetching that ref from the public clone url instead (read-only, no token).
    With neither, an orphan ``main`` with one empty commit is created, on which a patch whose
    first commit touches pre-existing files will not apply.
    """

    kind: Literal["local", "github"] = "local"

    def __init__(
        self,
        remote_dir: Path,
        run_dir: Path,
        base_repo: Path | None = None,
        base_ref: str = "main",
        seed_url: str | None = None,
        seed_ref: str = "main",
    ) -> None:
        # Absolute: the seeding push runs with cwd=base_repo, where a relative remote_dir would
        # not resolve.
        self.remote_dir = Path(remote_dir).resolve()
        self.run_dir = Path(run_dir).resolve()
        self.base_repo = Path(base_repo).resolve() if base_repo is not None else None
        self.base_ref = base_ref
        self.seed_url = seed_url
        self.seed_ref = seed_ref

    def fetch_issue(self, ref: str) -> Issue:
        """Only front-matter files are supported locally."""
        if ref.strip().isdigit():
            raise StageError("scm", "local scm cannot fetch GitHub issues")
        return parse_issue_file(Path(ref))

    def publish(
        self,
        *,
        branch: str,
        patch: bytes,
        title: str,
        body: str,
        labels: Sequence[str],
        allowed_prefixes: Sequence[str] | None = None,
        identity: PublicationIdentity | None = None,
    ) -> str:
        """Policy-check the patch, push ``branch`` into the bare remote, write/print ``pr.md``.

        The local remote is a real git repository, so it is the same compare-and-swap the GitHub
        path gets: a scripted or demo run of two instances against one bare repo behaves the way
        two managed factories against one GitHub repo behave.
        """
        _check_patch(patch, allowed_prefixes)
        self._ensure_remote()
        identity = identity or PublicationIdentity.from_branch(branch)
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
        meta = yaml.safe_dump({"id": slug, "title": title, "labels": list(labels)}, sort_keys=False).rstrip()
        path.write_text(f"---\n{meta}\n---\n{body.rstrip()}\n", encoding="utf-8")
        return f"file://{path.resolve()}"

    # -- internals

    def _ensure_remote(self) -> None:
        if not (self.remote_dir / "HEAD").exists():
            self.remote_dir.parent.mkdir(parents=True, exist_ok=True)
            _run(["git", "init", "--quiet", "--bare", str(self.remote_dir)], None)
        # A local demo/test may inspect or snapshot this bare remote immediately after publish.
        # Disable Git's asynchronous auto-maintenance so loose objects cannot be repacked out from
        # under that reader; explicit maintenance remains possible when an operator wants it.
        _run(["git", "config", "gc.auto", "0"], self.remote_dir)
        _run(["git", "config", "maintenance.auto", "false"], self.remote_dir)
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
        if self.seed_url:
            _run(
                ["git", "fetch", "--quiet", self.seed_url, f"{self.seed_ref}:refs/heads/main"],
                self.remote_dir,
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
        proc = subprocess.run(["git", "show-ref", "--heads"], cwd=self.remote_dir, capture_output=True, check=False)
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
        data = _gh_json(["gh", "issue", "view", ref.strip(), "--repo", self.repo, "--json", _ISSUE_FIELDS])
        return _issue_from_gh(data)  # type: ignore[arg-type]

    def list_open_issues(self, label: str, *, limit: int) -> list[Issue]:
        """The open issues carrying ``label``: a scheduled line's backlog (``intake_governance``).

        Open only: closed issues keep their label forever, and a scan window that fills with
        history is a scan that truncates the live work.
        """
        rows = _gh_json(
            [
                "gh", "issue", "list", "--repo", self.repo, "--label", label, "--state", "open",
                "--limit", str(limit + 1), "--json", _ISSUE_FIELDS,
            ]
        )  # fmt: skip
        rows = _window(list(rows), limit=limit, what=f"open issues carry label {label!r}")  # type: ignore[arg-type]
        return [_issue_from_gh(row) for row in rows]

    def list_open_pr_heads(self, *, limit: int) -> list[str]:
        """Head branches of every open PR; ``factory/<issue>-*`` among them is work in progress."""
        rows = _gh_json(
            [
                "gh", "pr", "list", "--repo", self.repo, "--state", "open",
                "--limit", str(limit + 1), "--json", "headRefName",
            ]
        )  # fmt: skip
        rows = _window(list(rows), limit=limit, what="open pull requests")  # type: ignore[arg-type]
        return [str(row.get("headRefName") or "") for row in rows]

    def publish(
        self,
        *,
        branch: str,
        patch: bytes,
        title: str,
        body: str,
        labels: Sequence[str],
        allowed_prefixes: Sequence[str] | None = None,
        identity: PublicationIdentity | None = None,
    ) -> str:
        """Policy check -> shallow clone of base -> checkout -b -> git am --3way -> push -> PR.

        An open PR for ``branch`` (a retried ``deliver``) is updated in place with ``gh pr edit``
        and its url returned; otherwise ``gh pr create`` opens one.
        """
        _check_patch(patch, allowed_prefixes)
        self._require_token()
        identity = identity or PublicationIdentity.from_branch(branch)
        key, marker_block = identity.key, identity.marker()
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
            # The marker travels in the body because the body is the one PR field every instance
            # can read and none can write without the repository credential it already needs.
            body_file.write_text(body + "\n\n" + marker_block + "\n", encoding="utf-8")
            if existing := (self._open_pr_url(branch) or self._adopted_pr_url(key)):
                _run(
                    [
                        "gh", "pr", "edit", existing, "--title", title,
                        "--body-file", str(body_file), *_label_flags(labels, "--add-label"),
                    ],
                    None,
                )  # fmt: skip
                return existing
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

    def observe_publication(self, branch: str) -> PublicationReceipt | None:
        """What the remote holds for ``branch``, bound to immutable git content; None if nothing.

        Every PR lifecycle state is read, because a closed or merged PR for the branch IS the
        publication: under an open-only lookup it read as absent and the replay opened a second
        one. When several PRs share the head, the live one describes the work (open, then merged,
        then closed). With no PR at all, a branch that exists is ``branch_only``, so a publication
        that died between the push and ``gh pr create`` resumes on that ref instead of being judged
        absent. The content digest comes from the PR's own diff (it outlives the merge and a deleted
        branch) or, for a bare branch, from the compare against the base -- never from the body.
        """
        out = _run(
            [
                "gh", "pr", "list", "--repo", self.repo, "--head", branch, "--state", "all",
                "--limit", "20", "--json", "number,url,state,headRefOid,baseRefName",
            ],
            None,
        )  # fmt: skip
        try:
            rows = json.loads(out or "[]")
        except ValueError as e:
            raise StageError("scm", f"gh pr list returned non-JSON: {out[:200]}", retryable=True) from e
        for state, pr_state in (("OPEN", "open"), ("MERGED", "merged"), ("CLOSED", "closed")):
            row = next((r for r in rows if isinstance(r, dict) and r.get("state") == state), None)
            if row is None:
                continue
            number = int(row["number"])
            diff = _run(["gh", "pr", "diff", str(number), "--repo", self.repo, "--patch"], None)
            return PublicationReceipt(
                repository=self.repo,
                base_revision=str(row.get("baseRefName") or ""),
                head_revision=str(row.get("headRefOid") or ""),
                content_digest=patch_content_digest(diff.encode()),
                branch=branch,
                pr_number=number,
                pr_state=pr_state,
                url=str(row.get("url") or "") or None,
            )
        out = _run(
            ["git", *self._cred, "ls-remote", f"https://github.com/{self.repo}.git", f"refs/heads/{branch}"],
            None,
        )
        first = out.split(maxsplit=1)
        if not first:
            return None
        head = first[0]
        diff = _run(
            [
                "gh", "api", "-H", "Accept: application/vnd.github.patch",
                f"repos/{self.repo}/compare/{self.base_branch}...{head}",
            ],
            None,
        )  # fmt: skip
        return PublicationReceipt(
            repository=self.repo,
            base_revision=self.base_branch,
            head_revision=head,
            content_digest=patch_content_digest(diff.encode()),
            branch=branch,
            pr_number=None,
            pr_state="branch_only",
        )

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

    def _adopted_pr_url(self, key: str) -> str | None:
        """Url of the open PR another instance already opened for this work, or None.

        Keyed on the marker in the body, not on the head branch. Two instances now converge on one
        branch, so the branch lookup usually suffices -- but a pull request opened before that
        change, or one whose branch was renamed, is still THIS work, and opening a second one for
        it is the failure this exists to prevent. The marker is parsed strictly (see
        `publication_identity.adopts`) so a key quoted in a log excerpt cannot adopt the wrong PR.
        """
        out = _run(
            [
                "gh", "pr", "list", "--repo", self.repo, "--state", "open",
                "--limit", "100", "--json", "url,body",
            ],
            None,
        )  # fmt: skip
        try:
            rows = json.loads(out or "[]")
        except ValueError:
            return None
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, dict) and adopts(str(row.get("body") or ""), key):
                return str(row.get("url") or "") or None
        return None

    def _open_pr_url(self, branch: str) -> str | None:
        """Url of the open PR whose head is ``branch``, or None."""
        out = _run(
            [
                "gh", "pr", "list", "--repo", self.repo, "--head", branch, "--state", "open",
                "--limit", "1", "--json", "url", "--jq", ".[].url",
            ],
            None,
        )  # fmt: skip
        lines = [line.strip() for line in out.splitlines() if line.strip()]
        return lines[0] if lines else None


def _label_flags(labels: Sequence[str], flag: str = "--label") -> list[str]:
    flags: list[str] = []
    for label in labels:
        flags += [flag, label]
    return flags


def _last_line(out: str) -> str:
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if not lines:
        raise StageError("scm", "gh returned no url")
    return lines[-1]


# ---------------------------------------------------------------- factory


def make_scm(cfg: Config, run_dir: Path, base_repo: Path | None = None, base_ref: str = "main") -> Scm:
    """Build the Scm named by ``cfg.scm``. ``base_repo``/``base_ref`` only matter for local.

    Without a host ``base_repo`` (islo sandbox) the local remote is seeded from the public clone
    url of ``cfg.repo`` at ``cfg.base_branch``, so the sandbox's patch stream still applies.
    """
    if cfg.scm == "github":
        return GitHubScm(cfg.repo, cfg.base_branch)
    seed_url = None if base_repo is not None else f"https://github.com/{cfg.repo}.git"
    return LocalGitScm(
        Path(run_dir) / "remote.git",
        Path(run_dir),
        base_repo,
        base_ref,
        seed_url=seed_url,
        seed_ref=cfg.base_branch,
    )
