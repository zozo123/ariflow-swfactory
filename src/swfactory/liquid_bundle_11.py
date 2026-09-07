from swfactory.liquid_bundle_engine import BundleSpec, Concern, DomainSpec, execute

BUNDLE = BundleSpec(
    "liquid400-01",
    755,
    804,
    (
        DomainSpec("authority-transfer", "authority", "authority"),
        DomainSpec("epoch-fencing", "airflow", "cells"),
        DomainSpec("airflow-recovery", "workgraph", "airflow_binding"),
        DomainSpec("mapped-task-cancel", "recovery", "lifecycle"),
        DomainSpec("approval-durability", "security", "lifecycle_evidence"),
    ),
    "Liquid400",
)
BUNDLE.validate()
_DOMAINS = {domain.slug: domain for domain in BUNDLE.domains}


def run(domain: str, concern: str, *, cell_id: str, epoch: int, **flags):
    return execute(_DOMAINS[domain], concern=Concern(concern), cell_id=cell_id, epoch=epoch, **flags)
