from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-08", 1105, 1154,
    (
        DomainSpec("fault-injection", "authority", "fault_evidence"),
        DomainSpec("benchmarks", "airflow", "evals"),
        DomainSpec("fuzzing", "workgraph", "evals"),
        DomainSpec("factory-generations", "recovery", "generations"),
        DomainSpec("stabilization-health", "security", "trust_evidence"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
