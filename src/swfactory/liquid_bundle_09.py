from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-09", 655, 704,
    (
        DomainSpec("factory-generations", "authority", "generations"),
        DomainSpec("self-improvement", "authority", "generations"),
        DomainSpec("planner-quality", "workgraph", "parallel_workers"),
        DomainSpec("agent-evaluation", "evidence", "evals"),
        DomainSpec("local-demo", "operator", "runtime"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
