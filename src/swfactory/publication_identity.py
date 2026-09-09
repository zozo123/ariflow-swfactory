"""Who owns a published branch, and which factory instance produced it.

Within one factory, concurrency is fenced by durable admission (#2058), the Cell epoch (#2041),
the single-writer operation journal (#2042) and the accepted-inputs pin (#2065). None of that
reaches ACROSS factories: two independently operated instances -- each with its own Airflow, its
own backend, its own state root -- share nothing but the GitHub repository they both work.

The publication path did not use that repository as the shared truth. The branch was
``factory/<issue>-<run_id>``, and ``run_id`` is per run, per instance, so two instances working
one issue opened two branches; PR reuse keyed on the branch, so two branches meant two pull
requests for one issue; and ``factory/*`` was force-pushed blind, so the second instance could
overwrite a commit a reviewer had already read on the first one's branch.

The key that identifies the work already exists and is instance-independent:
``CellIdentity.stable_id()`` is ``sha256(repo, target, issue)``. This module derives the same key
without needing a Cell store -- a direct run has no Cell and must still converge -- and carries it
onto the remote in two places a competing instance can see:

* the branch name, so every instance working one issue x target pushes the SAME ref, and git's
  own compare-and-swap (``--force-with-lease``) becomes the arbiter;
* a marker in the pull request body, so an instance adopts the PR that already exists instead of
  opening a second one.

No coordination service, no shared datastore, no new credential. The repository is the medium,
and the enforcement is git's, which is the only thing every instance is already obliged to obey.
"""

from __future__ import annotations

import hashlib
import os
import re
import uuid
from pathlib import Path

MARKER_START = "<!-- swf-publication"
MARKER_END = "-->"
INSTANCE_FILE = "instance.id"
_KEY_CHARS = 12


def publication_key(repo: str, target: str, issue: str) -> str:
    """The instance-independent identity of one issue x target's publication.

    Deliberately the same inputs, in the same order, as ``CellIdentity.stable_id``: two instances
    that disagree about which work is "the same work" would open two branches again, and the Cell
    id is the answer the rest of the factory already uses.
    """
    raw = f"{repo}\0{target}\0{issue}".encode()
    return hashlib.sha256(raw).hexdigest()[:_KEY_CHARS]


def instance_id(state_root: Path, *, create: bool = False) -> str:
    """This factory instance's stable id, created once under its own state root.

    Not the hostname: two instances legitimately share a host, one instance legitimately moves
    between them, and a container's hostname changes on every restart. The id lives beside the
    durable stores because that is exactly the thing that identifies an instance -- the state it
    owns -- and it is written once so a restart keeps its name.
    """
    path = Path(state_root) / INSTANCE_FILE
    try:
        existing = path.read_text(encoding="utf-8").strip()
    except OSError:
        existing = ""
    if existing:
        return existing
    override = os.environ.get("SWF_INSTANCE_ID", "").strip()
    if override:
        return override
    if not create:
        # `create=False` is the PUBLISH path, and it must not write. Under srt the state root is
        # inside the kernel's write-confined set, so minting a file here failed the whole run --
        # and a publication has no business creating state as a side effect anyway. The identity
        # is derived from the state root the instance owns, which is the thing that makes it a
        # distinct instance: two roots are two instances, one root keeps its name across restarts,
        # and no file is required for either to be true.
        digest = hashlib.sha256(str(Path(state_root).resolve()).encode()).hexdigest()
        return "swf-" + digest[:10]
    minted = "swf-" + uuid.uuid4().hex[:10]
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(minted + "\n", encoding="utf-8")
    except OSError:
        # An unwritable state root still gets a usable id for this process. Refusing to publish
        # over it would be a worse trade.
        return "swf-" + hashlib.sha256(str(Path(state_root).resolve()).encode()).hexdigest()[:10]
    return minted


def marker_block(*, key: str, instance: str, cell_id: str | None, epoch: int | None) -> str:
    """The block appended to a pull request body so another instance can recognise this work.

    An HTML comment: invisible to a reader, present in the body every instance can fetch, and not
    a field any instance can set without the repository credential it already needs to publish.
    """
    fields = [f"key={key}", f"instance={instance}"]
    if cell_id:
        fields.append(f"cell={cell_id}")
    if epoch is not None:
        fields.append(f"epoch={epoch}")
    return f"{MARKER_START} {' '.join(fields)} {MARKER_END}"


_MARKER_RE = re.compile(
    re.escape(MARKER_START) + r"\s+(?P<fields>[^>]*?)\s*" + re.escape(MARKER_END),
)


def read_marker(body: str) -> dict[str, str] | None:
    """Parse the marker out of a pull request body, or None when it carries none.

    Strict: a body that merely mentions a key in prose is not a marker. An unmarked PR is treated
    as belonging to nobody, which is what a pull request opened before this existed -- or by a
    person -- actually is.
    """
    found = _MARKER_RE.search(body or "")
    if not found:
        return None
    fields: dict[str, str] = {}
    for part in found.group("fields").split():
        name, _, value = part.partition("=")
        if name and value:
            fields[name] = value
    return fields if "key" in fields else None


def adopts(body: str, key: str) -> bool:
    """Whether the pull request with this body is the one for ``key``.

    Compares the parsed field, never a substring of the body: ``key=abc123`` must not adopt a PR
    whose body happens to quote ``abc1234`` somewhere in a log excerpt.
    """
    parsed = read_marker(body)
    return bool(parsed and parsed.get("key") == key)


class PublicationIdentity:
    """What a publication says about itself on the remote: its work key and its producer.

    Constructed by the caller that knows the Cell (``deliver``), or derived from the branch when a
    caller has only that -- an SCM implementation must never be the thing that decides what work
    it is publishing.
    """

    def __init__(self, *, key: str, instance: str, cell_id: str | None = None, epoch: int | None = None) -> None:
        self.key = key
        self.instance = instance
        self.cell_id = cell_id
        self.epoch = epoch

    @classmethod
    def from_branch(cls, branch: str) -> PublicationIdentity:
        """The identity implied by a ``factory/<issue>-<key>`` ref.

        The fallback for a caller that passes no identity. It keeps the marker truthful rather
        than absent: an unmarked pull request is indistinguishable from one a person opened, and
        would be adopted by nobody.
        """
        tail = branch.rsplit("-", 1)[-1] if "-" in branch else ""
        key = tail if len(tail) == _KEY_CHARS and all(c in "0123456789abcdef" for c in tail) else ""
        return cls(
            key=key or hashlib.sha256(branch.encode()).hexdigest()[:_KEY_CHARS], instance=instance_id(Path(".factory"))
        )

    def marker(self) -> str:
        return marker_block(key=self.key, instance=self.instance, cell_id=self.cell_id, epoch=self.epoch)
