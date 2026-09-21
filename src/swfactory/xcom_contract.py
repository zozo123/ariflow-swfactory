"""Machine-checked ownership contract for Airflow XCom documents.

XCom is scheduler scratch. It may carry routing metadata, bounded previews and untrusted HITL
responses, but never a durable authority seal, credential capability, raw patch, or secret.
"""

from __future__ import annotations

from typing import Any

FORBIDDEN_XCOM_KEYS = frozenset(
    {
        "policy_digest",
        "cell_policy_digest",
        "lease",
        "lease_id",
        "lease_handle",
        "credential",
        "credentials",
        "secret_env",
        "patch",
        "patch_b64",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "SWF_BACKEND_TOKEN",
    }
)


class XComContractError(ValueError):
    pass


def validate_xcom_document(value: Any, *, path: str = "$") -> None:
    """Reject authority-bearing fields recursively; values remain untrusted hints."""

    if isinstance(value, dict):
        for key, item in value.items():
            name = str(key)
            if name in FORBIDDEN_XCOM_KEYS:
                raise XComContractError(f"{path}.{name} is forbidden in Airflow XCom")
            validate_xcom_document(item, path=f"{path}.{name}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            validate_xcom_document(item, path=f"{path}[{index}]")
