from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-07",
    1055,
    1104,
    (
        DomainSpec("rollback", "workgraph", "generations"),
        DomainSpec("disaster-recovery", "recovery", "reconcile"),
        DomainSpec("cli-contract", "security", "cli"),
        DomainSpec("tui-parity", "evidence", "product_surface"),
        DomainSpec("backend-versioning", "operator", "contracts"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
