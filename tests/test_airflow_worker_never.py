import pytest

from swfactory.worker_security import WorkerPolicyViolation, assert_airflow_worker_boundary


@pytest.mark.parametrize(
    "action",
    [
        "github_publish",
        "github_raw_mutate",
        "credential_broker_redeem",
        "islo_login",
        "inject_secret_env",
    ],
)
def test_airflow_worker_refuses_trust_plane_actions(action: str) -> None:
    with pytest.raises(WorkerPolicyViolation, match="may never"):
        assert_airflow_worker_boundary(action=action)


@pytest.mark.parametrize("name", ["GH_TOKEN", "GITHUB_TOKEN", "SWF_BACKEND_TOKEN", "ISLO_API_KEY"])
def test_airflow_worker_refuses_authority_credentials(name: str) -> None:
    with pytest.raises(WorkerPolicyViolation, match="forbidden authority"):
        assert_airflow_worker_boundary(action="stage_compute", env={name: "secret"})


def test_airflow_worker_allows_scheduler_compute_without_authority() -> None:
    assert_airflow_worker_boundary(
        action="stage_compute",
        env={"AIRFLOW_CTX_DAG_ID": "software_factory", "PYTHONHASHSEED": "0"},
    )
