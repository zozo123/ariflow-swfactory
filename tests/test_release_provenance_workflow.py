from pathlib import Path


def test_release_attests_downloadable_artifacts_before_publication() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "actions/attest-build-provenance@v2" in workflow
    assert "release/provenance.json" in workflow
    assert "candidate-readiness.json" in workflow
    assert "swfactory.spdx.json" in workflow
    assert "swfactory.cdx.json" in workflow
    assert workflow.index("actions/attest-build-provenance@v2") < workflow.index("name: Create the GitHub Release")
    for target in (
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
    ):
        assert target + ".tar.gz" in workflow
    assert "py3-none-any.whl" in workflow


def test_supply_chain_no_longer_depends_on_release_published_event() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github" / "workflows" / "supply-chain.yml").read_text(encoding="utf-8")
    assert "types: [published]" not in workflow


def test_operator_docs_verify_the_downloaded_artifact() -> None:
    root = Path(__file__).resolve().parents[1]
    docs = (root / "docs" / "swf.md").read_text(encoding="utf-8")
    assert 'gh attestation verify "swf-$VERSION-$TARGET.tar.gz"' in docs
    assert "swfactory provenance verify --manifest provenance.json --root ." in docs
