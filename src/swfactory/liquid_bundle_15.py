from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-05", 955, 1004,
    (
        DomainSpec("storage-migration", "operator", "backend_store"),
        DomainSpec("evidence-sealing", "authority", "evidence_store"),
        DomainSpec("provenance-sbom", "airflow", "provenance"),
        DomainSpec("otel-correlation", "workgraph", "metrics"),
        DomainSpec("slo-budget", "recovery", "metrics"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
