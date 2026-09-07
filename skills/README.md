# Agent Skills

This repository publishes two installable Agent Skills for the Airflow Software Factory.

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

Both skills live under `skills/`, a standard discovery location used by the open `skills` CLI. Root `skills.sh.json` groups both published skill names so repository metadata matches CLI discovery.
