"""Executable repository topology discovery, materialization and immutable cache contracts."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
import time
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

import yaml

from swfactory.ci_topology import Module, Topology
from swfactory.workspace_materialization import MaterializationPlan, verify_sparse_coverage


@dataclass(frozen=True)
class OwnershipRule:
    pattern: str
    owners: tuple[str, ...]


@dataclass(frozen=True)
class WorkflowFilter:
    workflow: str
    paths: tuple[str, ...] = ()
    paths_ignore: tuple[str, ...] = ()


@dataclass(frozen=True)
class RepositoryTopology:
    modules: tuple[Module, ...]
    owners: tuple[OwnershipRule, ...]
    workflows: tuple[WorkflowFilter, ...]
    source_digest: str

    def ci_topology(self) -> Topology:
        modules = []
        for module in self.modules:
            checks = tuple(
                sorted(
                    workflow.workflow
                    for workflow in self.workflows
                    if any(
                        _glob_under(root, workflow.paths, workflow.paths_ignore)
                        for root in module.roots
                    )
                )
            )
            modules.append(
                Module(module.name, module.roots, module.depends_on, checks or module.checks)
            )
        return Topology(tuple(modules))

    def owners_for(self, path: str) -> tuple[str, ...]:
        normalized = _norm(path)
        owners: tuple[str, ...] = ()
        for rule in self.owners:
            if fnmatch.fnmatchcase(normalized, rule.pattern.lstrip("/")):
                owners = rule.owners
        return owners


@dataclass(frozen=True)
class MaterializationReceipt:
    repo_url: str
    base_sha: str
    requested_mode: str
    observed_mode: str
    destination: str
    required_paths: tuple[str, ...]
    fallback_reason: str | None
    duration_s: float
    bytes_on_disk: int
    schema_version: int = 1


@dataclass(frozen=True)
class CacheKey:
    namespace: str
    digest: str
    inputs: tuple[tuple[str, str], ...]

    @property
    def key(self) -> str:
        return f"{self.namespace}:{self.digest}"


@dataclass(frozen=True)
class CacheValidation:
    key: str
    status: str
    expected_sha256: str | None
    observed_sha256: str | None
    detail: str | None = None
    at: float = time.time()


class MaterializationError(RuntimeError):
    pass


def discover_repository(root: Path) -> RepositoryTopology:
    root = root.resolve()
    modules: list[Module] = []
    source_files: list[Path] = []

    cargo = root / "Cargo.toml"
    if cargo.is_file():
        source_files.append(cargo)
        data = tomllib.loads(cargo.read_text(encoding="utf-8"))
        workspace = data.get("workspace") if isinstance(data, dict) else None
        members = workspace.get("members", []) if isinstance(workspace, dict) else []
        if members:
            for member in members:
                member_root = _norm(str(member))
                modules.append(Module(name=f"cargo:{member_root}", roots=(member_root,)))
        else:
            package = data.get("package") if isinstance(data, dict) else None
            name = str(package.get("name", "root")) if isinstance(package, dict) else "root"
            modules.append(Module(name=f"cargo:{name}", roots=(".",)))

    pyproject = root / "pyproject.toml"
    if pyproject.is_file():
        source_files.append(pyproject)
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        project = data.get("project") if isinstance(data, dict) else None
        name = str(project.get("name", "python")) if isinstance(project, dict) else "python"
        roots = tuple(path for path in ("src",) if (root / path).exists()) or (".",)
        modules.append(Module(name=f"python:{name}", roots=roots))

    package = root / "package.json"
    if package.is_file():
        source_files.append(package)
        data = json.loads(package.read_text(encoding="utf-8"))
        workspaces = data.get("workspaces", []) if isinstance(data, dict) else []
        if isinstance(workspaces, dict):
            workspaces = workspaces.get("packages", [])
        if isinstance(workspaces, list) and workspaces:
            for item in workspaces:
                if isinstance(item, str):
                    modules.append(Module(name=f"npm:{item}", roots=(_norm(item.rstrip("/*")),)))
        else:
            modules.append(Module(name=f"npm:{data.get('name', 'root')}", roots=(".",)))

    owners_path = _find_codeowners(root)
    owners: tuple[OwnershipRule, ...] = ()
    if owners_path is not None:
        source_files.append(owners_path)
        owners = parse_codeowners(owners_path.read_text(encoding="utf-8"))

    workflows: list[WorkflowFilter] = []
    workflow_dir = root / ".github" / "workflows"
    if workflow_dir.is_dir():
        for path in sorted((*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml"))):
            source_files.append(path)
            workflows.append(parse_workflow_filter(path.name, path.read_text(encoding="utf-8")))

    # Deduplicate identical module roots produced by overlapping ecosystem manifests.
    unique: dict[tuple[str, tuple[str, ...]], Module] = {}
    for module in modules or [Module("root", (".",))]:
        unique[(module.name, module.roots)] = module
    digest = _files_digest(root, source_files)
    return RepositoryTopology(tuple(unique.values()), owners, tuple(workflows), digest)


def parse_codeowners(text: str) -> tuple[OwnershipRule, ...]:
    rules: list[OwnershipRule] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        pattern = parts[0].lstrip("/")
        owners = tuple(parts[1:])
        rules.append(OwnershipRule(pattern, owners))
    return tuple(rules)


def parse_workflow_filter(name: str, text: str) -> WorkflowFilter:
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError:
        return WorkflowFilter(name)
    if not isinstance(data, dict):
        return WorkflowFilter(name)
    trigger = data.get("on", data.get(True, {}))
    paths: list[str] = []
    ignored: list[str] = []
    if isinstance(trigger, dict):
        for conf in trigger.values():
            if not isinstance(conf, dict):
                continue
            p = conf.get("paths") or []
            i = conf.get("paths-ignore") or []
            if isinstance(p, list):
                paths.extend(str(item) for item in p)
            if isinstance(i, list):
                ignored.extend(str(item) for item in i)
    return WorkflowFilter(name, tuple(dict.fromkeys(paths)), tuple(dict.fromkeys(ignored)))


def materialize(
    repo_url: str,
    plan: MaterializationPlan,
    destination: Path,
    *,
    required_paths: Iterable[str],
) -> MaterializationReceipt:
    required = tuple(sorted({_norm(path) for path in required_paths if str(path).strip()}))
    covered, missing = verify_sparse_coverage(plan, required)
    if not covered:
        raise MaterializationError(f"materialization plan does not cover required paths: {missing}")
    started = time.monotonic()
    destination = destination.resolve()
    if destination.exists():
        raise MaterializationError(f"destination already exists: {destination}")
    fallback_reason: str | None = None
    observed = plan.mode
    try:
        _materialize_once(repo_url, plan, destination)
        _verify_checkout(destination, plan.base_sha, required)
    except (OSError, subprocess.CalledProcessError, MaterializationError) as exc:
        if plan.mode == "full":
            shutil.rmtree(destination, ignore_errors=True)
            raise
        fallback_reason = str(exc)[:1000]
        shutil.rmtree(destination, ignore_errors=True)
        observed = "full"
        full = MaterializationPlan(
            repo=plan.repo,
            base_sha=plan.base_sha,
            mode="full",
            sparse_paths=(),
            shallow=plan.shallow,
            partial_clone=False,
            cache_key=plan.cache_key,
        )
        _materialize_once(repo_url, full, destination)
        _verify_checkout(destination, plan.base_sha, required)
    return MaterializationReceipt(
        repo_url=repo_url,
        base_sha=plan.base_sha,
        requested_mode=plan.mode,
        observed_mode=observed,
        destination=str(destination),
        required_paths=required,
        fallback_reason=fallback_reason,
        duration_s=round(time.monotonic() - started, 6),
        bytes_on_disk=_tree_bytes(destination),
    )


def _materialize_once(repo_url: str, plan: MaterializationPlan, destination: Path) -> None:
    clone = ["git", "clone", "--no-checkout"]
    if plan.shallow:
        clone += ["--depth", "1"]
    if plan.partial_clone:
        clone += ["--filter=blob:none"]
    clone += [repo_url, str(destination)]
    _run(clone)
    if plan.mode == "sparse":
        _run(["git", "-C", str(destination), "sparse-checkout", "init", "--cone"])
        if plan.sparse_paths:
            _run(["git", "-C", str(destination), "sparse-checkout", "set", *plan.sparse_paths])
    # A SHA may not be a named branch, so explicitly fetch then detach FETCH_HEAD.
    fetch = ["git", "-C", str(destination), "fetch"]
    if plan.shallow:
        fetch += ["--depth", "1"]
    fetch += ["origin", plan.base_sha]
    _run(fetch)
    _run(["git", "-C", str(destination), "checkout", "--detach", "FETCH_HEAD"])


def _verify_checkout(destination: Path, base_sha: str, required: Iterable[str]) -> None:
    observed = _run(["git", "-C", str(destination), "rev-parse", "HEAD"], capture=True).strip()
    expected = _run(
        ["git", "-C", str(destination), "rev-parse", f"{base_sha}^{{commit}}"], capture=True
    ).strip()
    if observed != expected:
        raise MaterializationError(f"checkout head {observed} != expected {expected}")
    missing = [path for path in required if not (destination / path).exists()]
    if missing:
        raise MaterializationError(f"required paths missing after checkout: {sorted(missing)}")


def immutable_cache_key(namespace: str, inputs: Mapping[str, str]) -> CacheKey:
    normalized = tuple(sorted((str(key), str(value)) for key, value in inputs.items()))
    raw = json.dumps(normalized, separators=(",", ":"), ensure_ascii=True).encode()
    return CacheKey(namespace, hashlib.sha256(raw).hexdigest(), normalized)


def validate_cache_file(key: CacheKey, path: Path, expected_sha256: str | None) -> CacheValidation:
    if not path.is_file():
        return CacheValidation(key.key, "miss", expected_sha256, None, "cache entry is absent")
    observed = hashlib.sha256(path.read_bytes()).hexdigest()
    if expected_sha256 is not None and observed != expected_sha256:
        return CacheValidation(key.key, "quarantined", expected_sha256, observed, "sha256 mismatch")
    return CacheValidation(key.key, "hit", expected_sha256, observed)


def quarantine_cache(path: Path, validation: CacheValidation, quarantine_root: Path) -> Path:
    if validation.status != "quarantined":
        raise ValueError("only failed cache validations may be quarantined")
    quarantine_root.mkdir(parents=True, exist_ok=True)
    suffix = hashlib.sha256(validation.key.encode()).hexdigest()[:16]
    target = quarantine_root / f"{path.name}.{suffix}.bad"
    os.replace(path, target)
    metadata = target.with_suffix(target.suffix + ".json")
    metadata.write_text(
        json.dumps(asdict(validation), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return target


def _find_codeowners(root: Path) -> Path | None:
    for rel in ("CODEOWNERS", ".github/CODEOWNERS", "docs/CODEOWNERS"):
        path = root / rel
        if path.is_file():
            return path
    return None


def _files_digest(root: Path, paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(set(paths)):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _norm(path: str) -> str:
    return str(PurePosixPath(path.replace("\\", "/")))


def _glob_root(pattern: str) -> str:
    if pattern.endswith("/**"):
        return pattern[:-3].rstrip("/")
    return pattern.rstrip("/")


def _glob_under(root: str, paths: Iterable[str], ignored: Iterable[str]) -> bool:
    root = _norm(root).lstrip("./")
    path_patterns = tuple(paths)
    ignore_patterns = tuple(ignored)
    if path_patterns and not any(
        fnmatch.fnmatchcase(root, _glob_root(pattern)) for pattern in path_patterns
    ):
        return False
    return not any(fnmatch.fnmatchcase(root, _glob_root(pattern)) for pattern in ignore_patterns)


def _run(argv: list[str], *, capture: bool = False) -> str:
    proc = subprocess.run(argv, check=False, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, argv, proc.stdout, proc.stderr)
    return proc.stdout if capture else ""


def _tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
