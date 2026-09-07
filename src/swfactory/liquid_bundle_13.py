from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-03", 855, 904,
    (
        DomainSpec("cleanup-debt", "recovery", "cleanup_receipt"),
        DomainSpec("admission-fairness", "security", "durable_admission"),
        DomainSpec("queue-starvation", "evidence", "durable_admission"),
        DomainSpec("repo-races", "operator", "repo_coordination"),
        DomainSpec("git-publication", "authority", "backend_scm"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
