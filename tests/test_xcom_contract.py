from __future__ import annotations

import pytest

from swfactory.cell_runtime import bind_jobs, identity_for_job
from swfactory.xcom_contract import XComContractError, validate_xcom_document


def _job() -> dict[str, object]:
    return {
        "issue": "42",
        "repo": "acme/widgets",
        "dir": ".",
        "base_branch": "main",
        "job_idx": 0,
    }


def test_managed_fan_out_drops_authority_seals_before_xcom() -> None:
    job = _job()
    binding = {
        "job_idx": 0,
        "cell_id": identity_for_job(job).stable_id(),
        "epoch": 3,
        "repo": "acme/widgets",
        "snapshot_digest": "d" * 64,
        "policy_digest": "policy:v1:" + "a" * 64,
        "factory_generation": "candidate-7",
    }

    (mapped,) = bind_jobs([job], [binding])

    assert mapped["cell_id"] == binding["cell_id"]
    assert mapped["cell_epoch"] == 3
    assert mapped["cell_managed"] is True
    assert "cell_policy_digest" not in mapped
    assert "cell_generation" not in mapped
    assert "policy_digest" not in mapped


@pytest.mark.parametrize(
    "document",
    [
        {"policy_digest": "policy:v1:" + "a" * 64},
        {"lease_id": "lease_x"},
        {"cell_generation": 7},
        {"nested": {"factory_generation": "candidate-7"}},
        {"nested": {"GH_TOKEN": "secret"}},
        [{"patch_b64": "Zm9v"}],
    ],
)
def test_xcom_contract_rejects_authority_and_secret_fields(document: object) -> None:
    with pytest.raises(XComContractError, match="forbidden"):
        validate_xcom_document(document)

def test_managed_fan_out_validates_existing_job_fields_before_xcom() -> None:
    job = _job()
    job["GH_TOKEN"] = "must-not-reach-xcom"
    binding = {
        "job_idx": 0,
        "cell_id": identity_for_job(job).stable_id(),
        "epoch": 3,
        "repo": "acme/widgets",
        "snapshot_digest": "d" * 64,
        "policy_digest": "policy:v1:" + "a" * 64,
        "factory_generation": "candidate-7",
    }

    with pytest.raises(XComContractError, match="GH_TOKEN.*forbidden"):
        bind_jobs([job], [binding])
