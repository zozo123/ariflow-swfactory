"""Legacy backlog tranche 03: snapshot ranks 101-150."""

from swfactory.legacy_issue_runtime import LegacyArea, LegacyTranche, run
from swfactory.liquid_bundle_engine import Concern, ExecutionIntent

TRANCHE = LegacyTranche("legacy-03", 101, 150)
AREAS = (
    LegacyArea.WORKGRAPH,
    LegacyArea.AIRFLOW,
    LegacyArea.CELL,
    LegacyArea.SECURITY,
    LegacyArea.PERSISTENCE,
    LegacyArea.GITHUB,
    LegacyArea.EVIDENCE,
    LegacyArea.OPERATOR,
    LegacyArea.DEPLOYMENT,
    LegacyArea.GENERATIONS,
)


def execute_matrix(*, cell_id: str, epoch: int) -> tuple[ExecutionIntent, ...]:
    TRANCHE.validate()
    return tuple(run(area, concern, cell_id=cell_id, epoch=epoch) for area in AREAS for concern in Concern)
