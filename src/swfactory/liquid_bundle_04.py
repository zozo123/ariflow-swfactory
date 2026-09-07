from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-04",
    405,
    454,
    (
        DomainSpec("contract-versioning", "authority", "contracts"),
        DomainSpec("evidence-ledger", "evidence", "evidence_store"),
        DomainSpec("observability-otel", "evidence", "metrics"),
        DomainSpec("slo-alerting", "evidence", "metrics"),
        DomainSpec("cost-accounting", "evidence", "metrics"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
