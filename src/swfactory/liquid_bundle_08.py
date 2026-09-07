from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-08",
    605,
    654,
    (
        DomainSpec("conformance", "evidence", "provider_conformance"),
        DomainSpec("benchmarks", "evidence", "evals"),
        DomainSpec("performance", "workgraph", "metrics"),
        DomainSpec("cache", "workgraph", "state"),
        DomainSpec("artifacts", "evidence", "evidence_store"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
