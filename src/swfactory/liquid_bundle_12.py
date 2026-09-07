from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-02", 805, 854,
    (
        DomainSpec("sandbox-provision", "evidence", "sandbox"),
        DomainSpec("sandbox-reclaim", "operator", "cleanup_receipt"),
        DomainSpec("provider-drift", "authority", "provider_conformance"),
        DomainSpec("operation-replay", "airflow", "idempotency"),
        DomainSpec("mutation-observation", "workgraph", "idempotency"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
