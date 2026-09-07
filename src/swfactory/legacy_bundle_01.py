"""Legacy backlog tranche 01: snapshot ranks 1-50.

This module consolidates the oldest open architecture issues onto canonical runtime authorities.
It is bounded execution metadata; Apache Airflow remains the only lifecycle scheduler.
"""

from swfactory.legacy_issue_runtime import LegacyArea, LegacyTranche, run
from swfactory.liquid_bundle_engine import Concern, ExecutionIntent

TRANCHE = LegacyTranche("legacy-01", 1, 50)
AREAS = (
    LegacyArea.CELL,
    LegacyArea.AIRFLOW,
    LegacyArea.WORKGRAPH,
    LegacyArea.PERSISTENCE,
    LegacyArea.SECURITY,
    LegacyArea.EVIDENCE,
    LegacyArea.GITHUB,
    LegacyArea.DEPLOYMENT,
    LegacyArea.GENERATIONS,
    LegacyArea.OPERATOR,
)


def execute_matrix(*, cell_id: str, epoch: int) -> tuple[ExecutionIntent, ...]:
    TRANCHE.validate()
    return tuple(run(area, concern, cell_id=cell_id, epoch=epoch) for area in AREAS for concern in Concern)
