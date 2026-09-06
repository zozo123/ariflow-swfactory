"""Portable production deployment profile and drain/rollback semantics."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class DeploymentProfile:
    backend_image: str
    replicas: int
    airflow_url: str
    postgres_dsn_secret: str
    sandbox_capacity: int
    readiness_path: str = "/v1/doctor"
    liveness_path: str = "/v1/doctor"
    drain_timeout_s: int = 120

    def validate(self) -> None:
        if self.replicas < 1:
            raise ValueError("replicas must be >= 1")
        if self.sandbox_capacity < 1:
            raise ValueError("sandbox_capacity must be >= 1")
        if not self.backend_image or ":" not in self.backend_image:
            raise ValueError("backend_image must be versioned")
        if not self.airflow_url.startswith(("http://", "https://")):
            raise ValueError("airflow_url must be HTTP(S)")
        if not self.postgres_dsn_secret:
            raise ValueError("postgres DSN must be provided by secret reference")

    def to_values(self) -> dict:
        self.validate()
        return asdict(self)


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
