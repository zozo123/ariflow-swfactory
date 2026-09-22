"""Provider binding for executable population manifests.

This module maps provider-neutral PopulationTask objects onto concrete provider/model/runtime
choices. It does not schedule work and it does not execute providers. The binding is a deterministic
search configuration only; Airflow remains the scheduler and the provider cannot change task
identity, authority, or the manifest it was assigned.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from swfactory.population_manifest import (
    POPULATION_MANIFEST_AUTHORITY,
    PopulationManifest,
    PopulationManifestError,
    PopulationTask,
)

PROVIDER_BINDING_SCHEMA_VERSION = 1
PROVIDER_BINDING_AUTHORITY = POPULATION_MANIFEST_AUTHORITY


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ProviderChoiceSet:
    """Allowed concrete choices for one diversity axis."""

    provider: tuple[str, ...] = ()
    model: tuple[str, ...] = ()
    runtime: tuple[str, ...] = ()
    prompt: tuple[str, ...] = ()
    context: tuple[str, ...] = ()
    mutation: tuple[str, ...] = ()
    verifier: tuple[str, ...] = ()
    attack_surface: tuple[str, ...] = ()

    def validate(self) -> None:
        for axis, values in self.canonical_dict().items():
            if any(not value.strip() for value in values):
                raise PopulationManifestError(f"provider choices for {axis!r} must be nonempty strings")
            if len(set(values)) != len(values):
                raise PopulationManifestError(f"provider choices for {axis!r} must be distinct")

    def choices_for(self, axis: str) -> tuple[str, ...]:
        normalized = axis.replace("-", "_")
        value = getattr(self, normalized, ())
        if not isinstance(value, tuple):
            raise PopulationManifestError(f"provider choice axis {axis!r} is invalid")
        return value

    def canonical_dict(self) -> dict[str, list[str]]:
        return {
            "provider": list(self.provider),
            "model": list(self.model),
            "runtime": list(self.runtime),
            "prompt": list(self.prompt),
            "context": list(self.context),
            "mutation": list(self.mutation),
            "verifier": list(self.verifier),
            "attack_surface": list(self.attack_surface),
        }


@dataclass(frozen=True)
class BoundPopulationTask:
    """Concrete provider choices bound to an immutable population task."""

    task_id: str
    variant_digest: str
    provider: str | None
    model: str | None
    runtime: str | None
    prompt_variant: str | None
    context_variant: str | None
    mutation_variant: str | None
    verifier_variant: str | None
    attack_surface_variant: str | None
    binding_digest: str
    authority: str = PROVIDER_BINDING_AUTHORITY
    schema_version: int = PROVIDER_BINDING_SCHEMA_VERSION

    def canonical_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "task_id": self.task_id,
            "variant_digest": self.variant_digest,
            "provider": self.provider,
            "model": self.model,
            "runtime": self.runtime,
            "prompt_variant": self.prompt_variant,
            "context_variant": self.context_variant,
            "mutation_variant": self.mutation_variant,
            "verifier_variant": self.verifier_variant,
            "attack_surface_variant": self.attack_surface_variant,
        }

    def validate(self) -> None:
        if self.schema_version != PROVIDER_BINDING_SCHEMA_VERSION:
            raise PopulationManifestError("unsupported provider binding schema")
        if self.authority != PROVIDER_BINDING_AUTHORITY:
            raise PopulationManifestError("provider bindings must remain search-only")
        if not self.task_id.startswith("pop_"):
            raise PopulationManifestError("provider binding task id must use the pop_ namespace")
        if self.binding_digest != _digest(self.canonical_dict()):
            raise PopulationManifestError("provider binding digest mismatch")


@dataclass(frozen=True)
class ProviderBindingManifest:
    """Concrete provider choices for an entire population manifest."""

    population_manifest_digest: str
    tasks: tuple[BoundPopulationTask, ...]
    authority: str = PROVIDER_BINDING_AUTHORITY
    scheduler: str = "airflow"
    schema_version: int = PROVIDER_BINDING_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != PROVIDER_BINDING_SCHEMA_VERSION:
            raise PopulationManifestError("unsupported provider binding manifest schema")
        if self.authority != PROVIDER_BINDING_AUTHORITY:
            raise PopulationManifestError("provider binding manifest must remain search-only")
        if self.scheduler != "airflow":
            raise PopulationManifestError("provider binding manifest cannot introduce another scheduler")
        ids: set[str] = set()
        for task in self.tasks:
            task.validate()
            if task.task_id in ids:
                raise PopulationManifestError(f"duplicate bound population task {task.task_id}")
            ids.add(task.task_id)

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "authority": self.authority,
            "scheduler": self.scheduler,
            "population_manifest_digest": self.population_manifest_digest,
            "tasks": [task.canonical_dict() | {"binding_digest": task.binding_digest} for task in self.tasks],
        }

    def digest(self) -> str:
        return _digest(self.canonical_dict())


def provider_binding_manifest_from_document(
    document: Mapping[str, Any],
) -> ProviderBindingManifest:
    """Rehydrate one provider binding manifest and verify every bound task."""

    raw_tasks = document.get("tasks")
    if not isinstance(raw_tasks, list):
        raise PopulationManifestError("provider binding tasks must be an array")
    tasks = tuple(
        bound_population_task_from_document(row)
        for row in raw_tasks
        if isinstance(row, Mapping)
    )
    if len(tasks) != len(raw_tasks):
        raise PopulationManifestError("every provider binding task must be an object")
    manifest = ProviderBindingManifest(
        population_manifest_digest=str(document["population_manifest_digest"]),
        tasks=tasks,
        authority=str(document.get("authority", PROVIDER_BINDING_AUTHORITY)),
        scheduler=str(document.get("scheduler", "airflow")),
        schema_version=int(document.get("schema_version", PROVIDER_BINDING_SCHEMA_VERSION)),
    )
    manifest.validate()
    return manifest


def bound_population_task_from_document(document: Mapping[str, Any]) -> BoundPopulationTask:
    """Rehydrate one concrete provider binding and verify its binding digest."""

    task = BoundPopulationTask(
        task_id=str(document["task_id"]),
        variant_digest=str(document["variant_digest"]),
        provider=(str(document["provider"]) if document.get("provider") is not None else None),
        model=(str(document["model"]) if document.get("model") is not None else None),
        runtime=(str(document["runtime"]) if document.get("runtime") is not None else None),
        prompt_variant=(
            str(document["prompt_variant"])
            if document.get("prompt_variant") is not None
            else None
        ),
        context_variant=(
            str(document["context_variant"])
            if document.get("context_variant") is not None
            else None
        ),
        mutation_variant=(
            str(document["mutation_variant"])
            if document.get("mutation_variant") is not None
            else None
        ),
        verifier_variant=(
            str(document["verifier_variant"])
            if document.get("verifier_variant") is not None
            else None
        ),
        attack_surface_variant=(
            str(document["attack_surface_variant"])
            if document.get("attack_surface_variant") is not None
            else None
        ),
        binding_digest=str(document["binding_digest"]),
        authority=str(document.get("authority", PROVIDER_BINDING_AUTHORITY)),
        schema_version=int(document.get("schema_version", PROVIDER_BINDING_SCHEMA_VERSION)),
    )
    task.validate()
    return task


def provider_choices_from_document(document: Mapping[str, Any]) -> ProviderChoiceSet:
    """Load an allowlist document without accepting unknown axes or non-string choices."""

    allowed = {
        "provider",
        "model",
        "runtime",
        "prompt",
        "context",
        "mutation",
        "verifier",
        "attack_surface",
    }
    unknown = sorted(set(document) - allowed)
    if unknown:
        raise PopulationManifestError("unknown provider choice axes: " + ", ".join(unknown))

    values: dict[str, tuple[str, ...]] = {}
    for axis in allowed:
        raw = document.get(axis, ())
        if not isinstance(raw, (list, tuple)):
            raise PopulationManifestError(f"provider choices for {axis!r} must be an array")
        if any(not isinstance(value, str) for value in raw):
            raise PopulationManifestError(f"provider choices for {axis!r} must contain strings only")
        choices = tuple(value.strip() for value in raw)
        if any(not value for value in choices):
            raise PopulationManifestError(f"provider choices for {axis!r} must be nonempty strings")
        if len(set(choices)) != len(choices):
            raise PopulationManifestError(f"provider choices for {axis!r} must be distinct")
        values[axis] = choices
    return ProviderChoiceSet(**values)


def _pick(
    task: PopulationTask,
    choices: ProviderChoiceSet,
    axis: str,
) -> str | None:
    options = choices.choices_for(axis)
    if not options:
        return None
    coordinate = dict(task.diversity_coordinates).get(axis)
    if coordinate is None:
        return None
    return options[coordinate % len(options)]


def bind_population_manifest(
    manifest: PopulationManifest,
    *,
    choices: ProviderChoiceSet,
    required_axes: Sequence[str] = ("provider", "model", "runtime"),
) -> ProviderBindingManifest:
    """Bind provider-neutral diversity coordinates to concrete allowed provider choices.

    Required axes are enforced only when a task declares that axis. This keeps exact-replay or
    verifier lanes free to omit model/prompt variation while still preventing a declared provider
    axis from silently collapsing to one unspecified implementation.
    """

    manifest.validate()
    choices.validate()
    bound: list[BoundPopulationTask] = []
    for task in manifest.tasks:
        declared = set(task.diversity_axes)
        selected: Mapping[str, str | None] = {
            axis: _pick(task, choices, axis)
            for axis in (
                "provider",
                "model",
                "runtime",
                "prompt",
                "context",
                "mutation",
                "verifier",
                "attack-surface",
            )
        }
        missing = sorted(axis for axis in required_axes if axis in declared and not selected.get(axis))
        if missing:
            raise PopulationManifestError(
                f"{task.task_id}: declared diversity axes have no provider choices: {', '.join(missing)}"
            )
        document = {
            "schema_version": PROVIDER_BINDING_SCHEMA_VERSION,
            "authority": PROVIDER_BINDING_AUTHORITY,
            "task_id": task.task_id,
            "variant_digest": task.variant_digest,
            "provider": selected["provider"],
            "model": selected["model"],
            "runtime": selected["runtime"],
            "prompt_variant": selected["prompt"],
            "context_variant": selected["context"],
            "mutation_variant": selected["mutation"],
            "verifier_variant": selected["verifier"],
            "attack_surface_variant": selected["attack-surface"],
        }
        bound.append(
            BoundPopulationTask(
                task_id=task.task_id,
                variant_digest=task.variant_digest,
                provider=selected["provider"],
                model=selected["model"],
                runtime=selected["runtime"],
                prompt_variant=selected["prompt"],
                context_variant=selected["context"],
                mutation_variant=selected["mutation"],
                verifier_variant=selected["verifier"],
                attack_surface_variant=selected["attack-surface"],
                binding_digest=_digest(document),
            )
        )
    result = ProviderBindingManifest(
        population_manifest_digest=manifest.digest(),
        tasks=tuple(bound),
    )
    result.validate()
    return result
