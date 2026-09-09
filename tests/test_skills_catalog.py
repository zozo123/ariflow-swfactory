import json
from pathlib import Path

from swfactory.skills_connector import SKILLS_CLI_VERSION

ROOT = Path(__file__).resolve().parents[1]
SKILLS = ROOT / "skills"


def _frontmatter(path: Path) -> dict[str, str]:
    text = path.read_text()
    assert text.startswith("---\n"), f"{path} has no YAML frontmatter"
    _, raw, _body = text.split("---", 2)
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        key, separator, value = line.partition(":")
        assert separator, f"invalid frontmatter line in {path}: {line!r}"
        assert key in {"name", "description"}, f"unsupported frontmatter key in {path}: {key}"
        fields[key] = value.strip().strip('"').strip("'")
    assert set(fields) == {"name", "description"}, f"incomplete frontmatter in {path}: {fields}"
    return fields


def test_skills_sh_manifest_matches_every_public_skill() -> None:
    config = json.loads((ROOT / "skills.sh.json").read_text())
    assert config["$schema"] == "https://skills.sh/schemas/skills.sh.schema.json"

    published = {path.parent.name for path in SKILLS.glob("*/SKILL.md")}
    grouped = {
        slug
        for grouping in config["groupings"]
        for slug in grouping.get("skills", [])
    }
    assert published == {"airflow-software-factory", "swfactory"}
    assert grouped == published

    for slug in sorted(published):
        skill_dir = SKILLS / slug
        fields = _frontmatter(skill_dir / "SKILL.md")
        assert fields["name"] == slug
        assert fields["name"] == fields["name"].lower()
        assert fields["description"].strip()
        assert (skill_dir / "agents" / "openai.yaml").is_file(), f"{slug} lacks ChatGPT UI metadata"


def test_trusted_skills_cli_is_version_pinned_everywhere_it_executes() -> None:
    expected = f"skills@{SKILLS_CLI_VERSION}"
    connector = (ROOT / "src" / "swfactory" / "skills_connector.py").read_text()
    workflow = (ROOT / ".github" / "workflows" / "skills-catalog.yml").read_text()
    assert "skills@latest" not in connector
    assert "skills@latest" not in workflow
    assert expected in connector
    assert expected in workflow
