"""Compatibility adapter for pre-Liquid500 architecture issues.

Older backlog items used many one-off names but fall into the ten tracks defined by epic #65.
This adapter deliberately maps them onto the same canonical bundle engine used by Liquid400/500
so closing the legacy backlog does not preserve duplicate runtimes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from swfactory.liquid_bundle_engine import Concern, DomainSpec, ExecutionIntent, execute


class LegacyArea(StrEnum):
    CELL = "cell-control-plane"
    AIRFLOW = "airflow-lifecycle"
    OPERATOR = "operator"
    WORKGRAPH = "workgraph-sandbox"
    PERSISTENCE = "persistence-reconciliation"
    SECURITY = "security-policy"
    EVIDENCE = "evidence-observability"
    GITHUB = "github-intake-publication"
    DEPLOYMENT = "deployment-supply-chain"
    GENERATIONS = "factory-generations"


_AREA_SPECS = {
    LegacyArea.CELL: DomainSpec(LegacyArea.CELL, "authority", "cells"),
    LegacyArea.AIRFLOW: DomainSpec(LegacyArea.AIRFLOW, "airflow", "airflow_binding"),
    LegacyArea.OPERATOR: DomainSpec(LegacyArea.OPERATOR, "operator", "inspection"),
    LegacyArea.WORKGRAPH: DomainSpec(LegacyArea.WORKGRAPH, "workgraph", "parallel_workers"),
    LegacyArea.PERSISTENCE: DomainSpec(LegacyArea.PERSISTENCE, "recovery", "operation_recovery"),
    LegacyArea.SECURITY: DomainSpec(LegacyArea.SECURITY, "security", "security_contract"),
    LegacyArea.EVIDENCE: DomainSpec(LegacyArea.EVIDENCE, "evidence", "lifecycle_evidence"),
    LegacyArea.GITHUB: DomainSpec(LegacyArea.GITHUB, "recovery", "backend_scm"),
    LegacyArea.DEPLOYMENT: DomainSpec(LegacyArea.DEPLOYMENT, "operator", "runtime"),
    LegacyArea.GENERATIONS: DomainSpec(LegacyArea.GENERATIONS, "authority", "generations"),
}


@dataclass(frozen=True)
class LegacyTranche:
    tranche_id: str
    start_rank: int
    end_rank: int
    cutoff: str = "2026-09-06T22:22:21Z"

    @property
    def count(self) -> int:
        return self.end_rank - self.start_rank + 1

    def validate(self) -> None:
        if self.start_rank < 1 or self.count < 10 or self.count > 50:
            raise ValueError("legacy tranche must contain 10-50 open issues")


def run(
    area: LegacyArea,
    concern: Concern,
    *,
    cell_id: str,
    epoch: int,
    **flags,
) -> ExecutionIntent:
    return execute(_AREA_SPECS[area], concern=concern, cell_id=cell_id, epoch=epoch, **flags)
