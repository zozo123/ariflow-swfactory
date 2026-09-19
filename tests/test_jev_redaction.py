from swfactory.security_contract import REDACTED, redact_text


def test_typesafe_jev_api_key_is_redacted_from_free_form_diagnostics() -> None:
    fake = "apikey_" + "a" * 32 + "_" + "b" * 64
    message = f"provider failed credential={fake} retry=disabled"

    redacted = redact_text(message)

    assert fake not in redacted
    assert REDACTED in redacted
    assert "provider failed" in redacted
    assert "retry=disabled" in redacted
