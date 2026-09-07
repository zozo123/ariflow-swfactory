from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid500-01",
    255,
    304,
    (
        DomainSpec("cell-authority", "authority", "cells"),
        DomainSpec("airflow-lifecycle", "airflow", "airflow_binding"),
        DomainSpec("plan-work", "workgraph", "parallel_workers"),
        DomainSpec("sandbox-lifecycle", "workgraph", "sandbox"),
        DomainSpec("provider-capabilities", "workgraph", "provider_conformance"),
    ),
    "Liquid500",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
