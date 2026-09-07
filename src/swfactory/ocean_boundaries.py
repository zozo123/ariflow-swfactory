"""Boundary-layer admission and Cell conservation for Ocean120 D05-D06."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BoundaryRequest:
    cell_id: str
    epoch: int
    tenant: str
    trust_zone: str
    requested_units: int
    capacity_units: int
    scheduler: str = "airflow"

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_"):
            raise ValueError("canonical Cell identity required")
        if self.epoch < 1:
            raise ValueError("positive Cell epoch required")
        if not self.tenant or not self.trust_zone:
            raise ValueError("tenant and trust zone are required")
        if self.requested_units < 0 or self.capacity_units < 0:
            raise ValueError("capacity values cannot be negative")


@dataclass(frozen=True, slots=True)
class ConservationToken:
    cell_id: str
    epoch: int
    operation_key: str
    lineage: tuple[str, ...] = ()

    def validate(self) -> None:
        if not self.cell_id.startswith("cell_") or self.epoch < 1 or not self.operation_key:
            raise ValueError("invalid conservation token")


def admit_boundary(request: BoundaryRequest) -> tuple[bool, str]:
    request.validate()
    if request.scheduler != "airflow":
        return False, "Airflow is the only lifecycle scheduler"
    if request.requested_units > request.capacity_units:
        return False, "boundary capacity exceeded"
    return True, "boundary admitted"


def transfer_conservation(token: ConservationToken, replacement_compute_id: str) -> ConservationToken:
    """Carry durable work identity across disposable compute replacement."""

    token.validate()
    if not replacement_compute_id:
        raise ValueError("replacement compute identity required")
    return ConservationToken(
        cell_id=token.cell_id,
        epoch=token.epoch,
        operation_key=token.operation_key,
        lineage=(*token.lineage, replacement_compute_id),
    )
