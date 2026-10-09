import json

from swfactory.lifecycle_evidence import EvidenceWriter
from swfactory.security_contract import REDACTED, redact_text


def test_typesafe_jev_api_key_is_redacted_from_free_form_diagnostics() -> None:
    fake = "apikey_" + "a" * 32 + "_" + "b" * 64
    message = f"provider failed credential={fake} retry=disabled"

    redacted = redact_text(message)

    assert fake not in redacted
    assert REDACTED in redacted
    assert "provider failed" in redacted
    assert "retry=disabled" in redacted


def test_lifecycle_evidence_redacts_tokens_with_the_shared_vocabulary(tmp_path) -> None:
    github = "ghp_" + "a" * 36
    bearer = "Bearer " + "b" * 40
    record = EvidenceWriter(tmp_path).append(
        cell_id="cell_" + "0" * 24,
        epoch=1,
        kind="lifecycle_transition",
        payload={"reason": f"push failed with {github}", "headers": [f"Authorization: {bearer}"]},
    )

    stored = json.dumps(record)
    assert github not in stored
    assert bearer not in stored
    assert record["payload"] == {
        "reason": f"push failed with {REDACTED}",
        "headers": [f"Authorization: {REDACTED}"],
    }
