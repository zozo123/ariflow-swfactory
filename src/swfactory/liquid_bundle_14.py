from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-04", 905, 954,
    (
        DomainSpec("webhook-replay", "airflow", "webhook"),
        DomainSpec("intake-dedupe", "workgraph", "intake_policy"),
        DomainSpec("workspace-isolation", "recovery", "workspace_materialization"),
        DomainSpec("artifact-integrity", "security", "provenance"),
        DomainSpec("cache-consistency", "evidence", "state"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
