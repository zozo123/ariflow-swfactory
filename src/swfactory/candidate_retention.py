"""Promotion-window retention for candidate evidence bundles.

Candidate evidence is imported into a factory-owned content-addressed store and
covered by an explicit lease. Expired, unpinned bundles may be swept; pinned
bundles remain retained. The sweeper only removes digest-named directories under
its own root, so a retention record can never become an arbitrary-path deletion.

This module owns retention, not promotion. Pinning says "do not garbage collect
this evidence"; it does not approve or publish the candidate.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from swfactory.candidate_evidence import CandidateEvidenceBundle, verify_candidate_evidence_bundle


class CandidateRetentionError(RuntimeError):
    """Candidate evidence could not be retained or swept safely."""


@dataclass(frozen=True)
class RetentionLease:
    digest: str
    candidate_id: str
    output_head: str
    retained_at: str
    expires_at: str
    pinned: bool = False
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SweepReport:
    removed: tuple[str, ...]
    retained: tuple[str, ...]
    malformed: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def retain_candidate_evidence(
    bundle_path: Path,
    *,
    repo: Path,
    store: Path,
    ttl: timedelta,
    now: datetime | None = None,
) -> RetentionLease:
    """Import one verified candidate bundle and issue/refresh its retention lease."""
    if ttl <= timedelta(0):
        raise CandidateRetentionError("retention ttl must be positive")
    now = _utc(now)
    bundle = verify_candidate_evidence_bundle(bundle_path, repo=repo)
    digest = _digest_token(bundle)
    root, objects, leases = _layout(store)
    destination = objects / digest
    lease_path = leases / f"{digest}.json"

    if destination.exists():
        verify_candidate_evidence_bundle(destination, repo=repo)
    else:
        _copy_bundle(bundle_path.resolve(), destination, objects)

    current = _load_lease(lease_path)
    expiry = now + ttl
    if current is not None:
        if current.digest != digest or current.candidate_id != bundle.candidate_id:
            raise CandidateRetentionError(f"retention lease identity mismatch for {digest}")
        previous_expiry = _parse_time(current.expires_at)
        expiry = max(previous_expiry, expiry)
        pinned = current.pinned
        retained_at = current.retained_at
    else:
        pinned = False
        retained_at = _format_time(now)

    lease = RetentionLease(
        digest=digest,
        candidate_id=bundle.candidate_id,
        output_head=bundle.output_head,
        retained_at=retained_at,
        expires_at=_format_time(expiry),
        pinned=pinned,
    )
    _write_json(lease_path, lease.to_dict())
    _restrict_dir(root)
    return lease


def pin_candidate_evidence(store: Path, digest: str) -> RetentionLease:
    """Make retained evidence immune to expiry sweeps without promoting it."""
    _, objects, leases = _layout(store)
    token = _validate_digest_token(digest)
    lease_path = leases / f"{token}.json"
    lease = _require_lease(lease_path)
    if not (objects / token).is_dir():
        raise CandidateRetentionError(f"retained candidate evidence is absent: {token}")
    pinned = replace(lease, pinned=True)
    _write_json(lease_path, pinned.to_dict())
    return pinned


def unpin_candidate_evidence(store: Path, digest: str) -> RetentionLease:
    """Return retained evidence to ordinary lease expiry semantics."""
    _, objects, leases = _layout(store)
    token = _validate_digest_token(digest)
    lease_path = leases / f"{token}.json"
    lease = _require_lease(lease_path)
    if not (objects / token).is_dir():
        raise CandidateRetentionError(f"retained candidate evidence is absent: {token}")
    unpinned = replace(lease, pinned=False)
    _write_json(lease_path, unpinned.to_dict())
    return unpinned


def sweep_candidate_evidence(
    store: Path,
    *,
    now: datetime | None = None,
    dry_run: bool = False,
) -> SweepReport:
    """Remove only expired, unpinned objects owned by this retention store."""
    now = _utc(now)
    _, objects, leases = _layout(store)
    removed: list[str] = []
    retained: list[str] = []
    malformed: list[str] = []

    for lease_path in sorted(leases.glob("*.json")):
        token = lease_path.stem
        try:
            _validate_digest_token(token)
            lease = _require_lease(lease_path)
            if lease.digest != token:
                raise CandidateRetentionError("lease filename and digest disagree")
            destination = objects / token
            if not destination.is_dir():
                raise CandidateRetentionError("lease points to absent object")
            expired = _parse_time(lease.expires_at) <= now
        except (CandidateRetentionError, ValueError):
            malformed.append(token)
            continue

        if lease.pinned or not expired:
            retained.append(token)
            continue
        if dry_run:
            removed.append(token)
            continue
        try:
            _safe_remove_object(objects, destination, token)
        except CandidateRetentionError:
            malformed.append(token)
            continue
        lease_path.unlink(missing_ok=True)
        removed.append(token)

    return SweepReport(tuple(removed), tuple(retained), tuple(malformed))


def verify_retained_candidate_evidence(
    store: Path,
    digest: str,
    *,
    repo: Path,
) -> CandidateEvidenceBundle:
    """Verify the retained bytes and their frozen Git candidate ref."""
    _, objects, leases = _layout(store)
    token = _validate_digest_token(digest)
    lease = _require_lease(leases / f"{token}.json")
    if lease.digest != token:
        raise CandidateRetentionError("lease filename and digest disagree")
    bundle = verify_candidate_evidence_bundle(objects / token, repo=repo)
    if _digest_token(bundle) != token:
        raise CandidateRetentionError("retained bundle manifest digest differs from object identity")
    if bundle.candidate_id != lease.candidate_id or bundle.output_head != lease.output_head:
        raise CandidateRetentionError("retention lease differs from retained candidate evidence")
    return bundle


def _layout(store: Path) -> tuple[Path, Path, Path]:
    root = store.resolve()
    objects = root / "objects"
    leases = root / "leases"
    for directory in (root, objects, leases):
        directory.mkdir(parents=True, exist_ok=True)
        _restrict_dir(directory)
    return root, objects, leases


def _copy_bundle(source: Path, destination: Path, objects: Path) -> None:
    if source.is_symlink() or not source.is_dir():
        raise CandidateRetentionError(f"candidate evidence bundle is not a regular directory: {source}")
    for entry in source.rglob("*"):
        if entry.is_symlink():
            raise CandidateRetentionError(f"candidate evidence bundle contains a symlink: {entry}")
    temporary = Path(tempfile.mkdtemp(prefix=".retain-", dir=objects))
    try:
        shutil.copytree(source, temporary, dirs_exist_ok=True, copy_function=shutil.copyfile)
        for entry in temporary.rglob("*"):
            if entry.is_dir():
                _restrict_dir(entry)
            elif entry.is_file():
                _restrict_file(entry)
        try:
            os.replace(temporary, destination)
        except FileExistsError:
            shutil.rmtree(temporary, ignore_errors=True)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)


def _safe_remove_object(objects: Path, destination: Path, token: str) -> None:
    expected = (objects / token).resolve()
    if destination.is_symlink() or destination.resolve() != expected:
        raise CandidateRetentionError(f"refusing redirected retention object: {destination}")
    if expected.parent != objects.resolve():
        raise CandidateRetentionError(f"refusing retention object outside store: {expected}")
    shutil.rmtree(expected)


def _digest_token(bundle: CandidateEvidenceBundle) -> str:
    digest = bundle.digest()
    prefix = "sha256:"
    if not digest.startswith(prefix):
        raise CandidateRetentionError(f"unsupported candidate evidence digest: {digest}")
    return _validate_digest_token(digest.removeprefix(prefix))


def _validate_digest_token(token: str) -> str:
    if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
        raise CandidateRetentionError(f"invalid candidate evidence digest: {token!r}")
    return token


def _load_lease(path: Path) -> RetentionLease | None:
    if not path.is_file():
        return None
    return _parse_lease(path)


def _require_lease(path: Path) -> RetentionLease:
    if not path.is_file() or path.is_symlink():
        raise CandidateRetentionError(f"retention lease is absent or unsafe: {path}")
    return _parse_lease(path)


def _parse_lease(path: Path) -> RetentionLease:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        lease = RetentionLease(
            digest=str(document["digest"]),
            candidate_id=str(document["candidate_id"]),
            output_head=str(document["output_head"]),
            retained_at=str(document["retained_at"]),
            expires_at=str(document["expires_at"]),
            pinned=bool(document.get("pinned", False)),
            schema_version=int(document.get("schema_version", 1)),
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise CandidateRetentionError(f"cannot load retention lease {path}: {error}") from error
    if lease.schema_version != 1:
        raise CandidateRetentionError(f"unsupported retention lease schema {lease.schema_version}")
    _validate_digest_token(lease.digest)
    _parse_time(lease.retained_at)
    _parse_time(lease.expires_at)
    return lease


def _write_json(path: Path, document: dict[str, Any]) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _restrict_file(temporary)
        os.replace(temporary, path)
        _restrict_file(path)
    finally:
        temporary.unlink(missing_ok=True)


def _utc(value: datetime | None) -> datetime:
    value = datetime.now(UTC) if value is None else value
    if value.tzinfo is None:
        raise CandidateRetentionError("retention timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _format_time(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise CandidateRetentionError(f"retention timestamp is not timezone-aware: {value!r}")
    return parsed.astimezone(UTC)


def _restrict_dir(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o700)


def _restrict_file(path: Path) -> None:
    if os.name == "posix":
        path.chmod(0o600)
