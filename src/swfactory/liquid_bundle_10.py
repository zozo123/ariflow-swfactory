from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-10", 705, 754,
    (
        DomainSpec("docs-dx", "operator", "inspection"),
        DomainSpec("test-architecture", "evidence", "ci_topology"),
        DomainSpec("fuzz-property", "evidence", "evals"),
        DomainSpec("architecture-health", "authority", "authority"),
        DomainSpec("stabilization-readiness", "operator", "trust_evidence"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
