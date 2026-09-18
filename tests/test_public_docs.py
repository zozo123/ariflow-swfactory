from pathlib import Path


def test_public_operator_docs_have_no_template_placeholders() -> None:
    root = Path(__file__).resolve().parents[1]
    public = (root / "docs" / "swf.md").read_text(encoding="utf-8")
    placeholders = ("FILTERS_GATES", "FILTERS_JOBS", "FILTERS_RUNS", "EXAMPLES_BLOCK", "DRYRUN_PARA")
    assert not any(marker in public for marker in placeholders)
    assert "swf gates approve --all" in public
    assert "--dry-run" in public
    assert "--limit" in public
    assert "truncated" in public


def test_readme_links_current_and_historical_improvement_plans() -> None:
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    assert "issues/2040" in readme
    assert "current execution plan" in readme
    assert "issues/2022" in readme
    assert "historical context" in readme
