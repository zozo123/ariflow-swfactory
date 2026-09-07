from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-05", 455, 504,
    (
        DomainSpec("security-boundaries", "security", "security_boundary"),
        DomainSpec("secret-brokerage", "security", "security_contract"),
        DomainSpec("policy-engine", "security", "security_contract"),
        DomainSpec("multitenancy", "security", "worker_security"),
        DomainSpec("provenance-sbom", "security", "provenance"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
