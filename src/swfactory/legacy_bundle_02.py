"""Legacy backlog tranche 02: snapshot ranks 51-100."""

from swfactory.legacy_issue_runtime import LegacyArea, LegacyTranche, run
from swfactory.liquid_bundle_engine import Concern, ExecutionIntent

TRANCHE = LegacyTranche("legacy-02", 51, 100)
AREAS = (
    LegacyArea.PERSISTENCE,
    LegacyArea.EVIDENCE,
    LegacyArea.GITHUB,
    LegacyArea.DEPLOYMENT,
    LegacyArea.OPERATOR,
    LegacyArea.SECURITY,
    LegacyArea.CELL,
    LegacyArea.AIRFLOW,
    LegacyArea.WORKGRAPH,
    LegacyArea.GENERATIONS,
)


def execute_matrix(*, cell_id: str, epoch: int) -> tuple[ExecutionIntent, ...]:
    TRANCHE.validate()
    return tuple(
        run(area, concern, cell_id=cell_id, epoch=epoch)
        for area in AREAS
        for concern in Concern
    )
