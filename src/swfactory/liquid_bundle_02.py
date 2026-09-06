from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-02", 305, 354,
    (
        DomainSpec("mutation-journal", "recovery", "idempotency"),
        DomainSpec("cleanup-reconciliation", "recovery", "operation_recovery"),
        DomainSpec("admission-backpressure", "authority", "durable_admission"),
        DomainSpec("github-intake", "recovery", "intake_policy"),
        DomainSpec("github-publication", "recovery", "backend_scm"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
