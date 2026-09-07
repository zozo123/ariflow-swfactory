"""Legacy backlog tranche 04: snapshot ranks 151-181."""

from swfactory.legacy_issue_runtime import LegacyArea, LegacyTranche, run
from swfactory.liquid_bundle_engine import Concern, ExecutionIntent

TRANCHE = LegacyTranche("legacy-04", 151, 181)
AREAS = (
    LegacyArea.GENERATIONS,
    LegacyArea.OPERATOR,
    LegacyArea.EVIDENCE,
    LegacyArea.SECURITY,
    LegacyArea.PERSISTENCE,
    LegacyArea.WORKGRAPH,
    LegacyArea.AIRFLOW,
    LegacyArea.CELL,
    LegacyArea.GITHUB,
    LegacyArea.DEPLOYMENT,
)


def execute_matrix(*, cell_id: str, epoch: int) -> tuple[ExecutionIntent, ...]:
    TRANCHE.validate()
    return tuple(run(area, concern, cell_id=cell_id, epoch=epoch) for area in AREAS for concern in Concern)
