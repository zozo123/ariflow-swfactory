from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-07",
    555,
    604,
    (
        DomainSpec("backend-api", "operator", "backend"),
        DomainSpec("config-validation", "operator", "contracts"),
        DomainSpec("webhooks", "recovery", "webhook"),
        DomainSpec("human-gates", "airflow", "lifecycle_evidence"),
        DomainSpec("fault-injection", "evidence", "fault_evidence"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
