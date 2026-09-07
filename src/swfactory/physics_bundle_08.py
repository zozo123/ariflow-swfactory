from swfactory.liquid_bundle_engine import Concern
from swfactory.physics_wave_runtime import PhysicsWave, WaveBundle, execute_wave

BUNDLE = WaveBundle("phase-05", PhysicsWave.PHASE240, 17, 20)
BUNDLE.validate()


def run(domain_ordinal: int, domain_slug: str, concern: str, *, owner: str, cell_id: str, epoch: int, **flags):
    if not BUNDLE.contains(domain_ordinal):
        raise ValueError("domain is outside phase bundle 05")
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
