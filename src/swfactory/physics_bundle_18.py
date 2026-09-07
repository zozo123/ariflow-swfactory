from swfactory.liquid_bundle_engine import Concern
from swfactory.physics_wave_runtime import PhysicsWave, WaveBundle, execute_wave

BUNDLE = WaveBundle("statmech-09", PhysicsWave.STATMECH360, 33, 36)
BUNDLE.validate()


def run(domain_ordinal: int, domain_slug: str, concern: str, *, owner: str, cell_id: str, epoch: int, **flags):
    if not BUNDLE.contains(domain_ordinal):
        raise ValueError("domain is outside statmech bundle 09")
    return execute_wave(
        BUNDLE.wave,
        domain_ordinal=domain_ordinal,
        domain_slug=domain_slug,
        owner=owner,
        concern=Concern(concern),
        cell_id=cell_id,
        epoch=epoch,
        **flags,
    )
