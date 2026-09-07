from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-06",
    1005,
    1054,
    (
        DomainSpec("cost-attribution", "security", "metrics"),
        DomainSpec("secret-scope", "evidence", "security_contract"),
        DomainSpec("policy-drift", "operator", "security_boundary"),
        DomainSpec("tenant-isolation", "authority", "worker_security"),
        DomainSpec("release-promotion", "airflow", "generations"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
