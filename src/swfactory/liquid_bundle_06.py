from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-06",
    505,
    554,
    (
        DomainSpec("release-promotion", "operator", "generations"),
        DomainSpec("deployment-ha", "operator", "runtime"),
        DomainSpec("disaster-recovery", "recovery", "reconcile"),
        DomainSpec("operator-cli", "operator", "cli"),
        DomainSpec("operator-tui", "operator", "product_surface"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
