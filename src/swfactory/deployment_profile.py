"""The supported deployment boundary, and the drain/rollback semantics that live inside it.

One boundary is supported and qualified: **a single host, one shared local state root**. The
factory's five authoritative stores are SQLite files and an evidence tree on that host's local
filesystem, fenced by POSIX advisory locks and SQLite's own write lock.

This module used to describe replicas and a Postgres DSN while the running factory kept local
SQLite. That is not a harmless aspiration: an operator who scales that profile to two replicas gets
two backends, each opening its *own* copy of the four stores and each believing it holds the
authoritative operation journal. Both then re-drive the same in-doubt publication. So the
unqualified topology is refused here rather than described, and the state root is checked for the
properties SQLite actually needs.

Distributed/Postgres operation stays unqualified. Qualifying it means implementing it, not
relaxing ``validate``.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

SINGLE_HOST = "single-host-local-state"
MULTI_HOST_POSTGRES = "multi-host-postgres"
TOPOLOGIES = (SINGLE_HOST, MULTI_HOST_POSTGRES)

# Filesystems on which SQLite's locking is either advisory-only or silently broken. A factory whose
# state root lives on one of these can hand two writers the same "exclusive" lock, which is the
# distributed failure this deployment boundary exists to keep out.
UNSUPPORTED_STATE_FILESYSTEMS = frozenset(
    {"nfs", "nfs3", "nfs4", "smbfs", "cifs", "smb3", "9p", "fuse.sshfs", "afpfs", "ceph", "glusterfs"}
)

LINUX_MOUNTS = Path("/proc/self/mounts")


class UnqualifiedDeployment(ValueError):
    """A deployment shape this build does not implement, and therefore refuses to promise."""


class UnsupportedStateRoot(ValueError):
    """The state root cannot hold the factory's authoritative stores safely."""


@dataclass(frozen=True)
class DeploymentProfile:
    backend_image: str
    replicas: int
    airflow_url: str
    sandbox_capacity: int
    state_root: str = ".factory"
    topology: str = SINGLE_HOST
    postgres_dsn_secret: str = ""
    readiness_path: str = "/v1/doctor"
    liveness_path: str = "/v1/doctor"
    drain_timeout_s: int = 120

    def validate(self) -> None:
        if self.topology not in TOPOLOGIES:
            raise ValueError(f"topology must be one of {list(TOPOLOGIES)}")
        if self.topology != SINGLE_HOST:
            raise UnqualifiedDeployment(
                f"{self.topology!r} is unqualified: the factory's Cell, operation, admission and "
                "repair stores are local SQLite files with a local evidence tree. Run the supported "
                f"{SINGLE_HOST!r} profile; see docs/backup-restore.md."
            )
        if self.replicas != 1:
            raise UnqualifiedDeployment(
                f"replicas must be 1 on {SINGLE_HOST!r}: a second backend replica opens its own copy "
                "of the same local stores, and two copies of one operation journal both re-drive the "
                "same in-doubt external effect."
            )
        if self.postgres_dsn_secret:
            raise UnqualifiedDeployment(
                "a Postgres DSN promises a datastore this build does not implement; leave it unset."
            )
        if self.sandbox_capacity < 1:
            raise ValueError("sandbox_capacity must be >= 1")
        if not self.backend_image or ":" not in self.backend_image:
            raise ValueError("backend_image must be versioned")
        if not self.airflow_url.startswith(("http://", "https://")):
            raise ValueError("airflow_url must be HTTP(S)")
        if not self.state_root.strip():
            raise ValueError("state_root must name the one shared local state directory")

    def to_values(self) -> dict:
        self.validate()
        return asdict(self)


def read_linux_mounts() -> str:
    """Return the kernel mount table, or ``''`` where there is none to read (macOS, restricted)."""
    try:
        return LINUX_MOUNTS.read_text(encoding="utf-8")
    except OSError:
        return ""


def filesystem_type(path: Path, *, mounts: Callable[[], str] = read_linux_mounts) -> str:
    """Best-effort filesystem type for ``path``; ``'unknown'`` when the host does not say.

    Unknown is deliberately not a refusal: refusing every host whose mount table cannot be read
    would reject working macOS and container deployments, and a false refusal teaches operators to
    pass a bypass flag, which costs more safety than this check buys.
    """
    table = mounts()
    if not table:
        return "unknown"
    resolved = Path(path).resolve()
    best: tuple[int, str] = (-1, "unknown")
    for line in table.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        mount_point, fs_type = fields[1], fields[2]
        try:
            candidate = Path(mount_point.replace("\\040", " ")).resolve()
        except OSError:  # pragma: no cover - unreadable mount point
            continue
        if resolved == candidate or candidate in resolved.parents:
            depth = len(candidate.parts)
            if depth > best[0]:
                best = (depth, fs_type)
    return best[1]


def assert_supported_state_root(state_root: Path, *, mounts: Callable[[], str] = read_linux_mounts) -> str:
    """Refuse a state root the supported boundary cannot be enforced on. Returns its filesystem.

    Two properties are checked, both of which the factory depends on rather than merely prefers:
    POSIX advisory locking must work (SQLite and the run lock are built on it), and the filesystem
    must not be one whose locking is known to be unreliable across hosts.
    """
    root = Path(state_root)
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir():
        raise UnsupportedStateRoot(f"{root} is not a directory")
    fs_type = filesystem_type(root, mounts=mounts)
    if fs_type in UNSUPPORTED_STATE_FILESYSTEMS:
        raise UnsupportedStateRoot(
            f"{root} is on {fs_type}, where SQLite's locking cannot fence a second writer. Put the "
            "factory state root on a local filesystem; the supported boundary is one host with one "
            "shared local state directory."
        )
    probe = root / ".lockprobe"
    try:
        handle = os.open(probe, os.O_CREAT | os.O_RDWR, 0o600)
    except OSError as error:
        raise UnsupportedStateRoot(f"{root} is not writable: {error}") from error
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle, fcntl.LOCK_UN)
    except OSError as error:
        raise UnsupportedStateRoot(
            f"{root} does not support POSIX advisory locking ({error}); the factory cannot fence a second writer there."
        ) from error
    finally:
        os.close(handle)
        probe.unlink(missing_ok=True)
    return fs_type


@dataclass(frozen=True)
class DrainPlan:
    from_generation: str
    to_generation: str
    max_inflight_old: int
    rollback_generation: str

    def allowed_to_cutover(self, inflight_old: int) -> bool:
        return inflight_old <= self.max_inflight_old


def rollback_target(history: list[str], current: str) -> str:
    if current not in history:
        raise ValueError("current generation is not in deployment history")
    index = history.index(current)
    if index == 0:
        raise ValueError("no prior generation available")
    return history[index - 1]
