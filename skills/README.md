# Agent Skills

This repository publishes two installable Agent Skills for the Airflow Software Factory.

[![skills.sh](https://skills.sh/b/zozo123/ariflow-swfactory)](https://skills.sh/zozo123/ariflow-swfactory)

## Install

Operate, extend, stabilize, or audit the factory:

```bash
npx skills add https://github.com/zozo123/ariflow-swfactory --skill airflow-software-factory
```

Drive the governed factory from an outer coding harness through `swf`:

```bash
npx skills add https://github.com/zozo123/ariflow-swfactory --skill swfactory
```

Install both:

```bash
npx skills add https://github.com/zozo123/ariflow-swfactory --skill airflow-software-factory --skill swfactory
```

Sources:
- [`airflow-software-factory/SKILL.md`](airflow-software-factory/SKILL.md)
- [`swfactory/SKILL.md`](swfactory/SKILL.md)

Catalog pages:
- https://skills.sh/zozo123/ariflow-swfactory/airflow-software-factory
- https://skills.sh/zozo123/ariflow-swfactory/swfactory

Both skills live under `skills/`, the standard discovery location used by Vercel's open `skills` CLI. Root [`skills.sh.json`](../skills.sh.json) groups both published skill names so repository metadata matches CLI discovery. The repository-level [`.claude-plugin/plugin.json`](../.claude-plugin/plugin.json) exposes the same package to plugin-aware hosts.

## Connect to Vercel's skills repository

The trusted operator side can use Vercel's canonical discovery skill without turning it into a lifecycle scheduler:

```bash
npx skills add vercel-labs/skills@find-skills -y
npx skills find airflow --owner vercel-labs
```

`src/swfactory/skills_connector.py` provides shell-free command construction and canonical identities for:

- `zozo123/ariflow-swfactory@airflow-software-factory`
- `zozo123/ariflow-swfactory@swfactory`
- `vercel-labs/skills@find-skills`

See [`airflow-software-factory/references/skills-sh.md`](airflow-software-factory/references/skills-sh.md) for indexing verification and the trust boundary.
