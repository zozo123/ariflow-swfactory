from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-03",
    355,
    404,
    (
        DomainSpec("repo-coordination", "authority", "repo_coordination"),
        DomainSpec("ci-topology", "workgraph", "ci_topology"),
        DomainSpec("workspace-materialization", "workgraph", "workspace_materialization"),
        DomainSpec("storage-backend", "recovery", "backend_store"),
        DomainSpec("schema-migrations", "recovery", "backend_store"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
