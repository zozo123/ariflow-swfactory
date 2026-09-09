# skills.sh and Vercel Skills integration

Use this reference when publishing the repository skills, diagnosing catalog discovery, or pulling a reusable skill into an operator workflow.

## Canonical sources

- Factory skill: `zozo123/ariflow-swfactory@airflow-software-factory`
- Outer-harness skill: `zozo123/ariflow-swfactory@swfactory`
- Vercel discovery skill: `vercel-labs/skills@find-skills`
- Vercel source repository: `https://github.com/vercel-labs/skills`
- Public catalog: `https://skills.sh/`

The factory skill is a capability package, not a scheduler. Apache Airflow remains the only lifecycle scheduler and Factory Cells retain durable identity/evidence authority.

## Operator connector

`swfactory.skills_connector` wraps Vercel's open `skills` CLI without shell interpolation. It keeps networked skill acquisition on the trusted operator/orchestrator side rather than giving a coding worker package-install or publication authority.

The connector and catalog CI pin the executable package to `skills@1.5.25`. Do not replace that pin with `skills@latest`: the CLI executes on a trusted host, so an unreviewed upstream publish would otherwise change executable behavior without changing this repository. Upgrade the pin deliberately in the connector, its tests, and `.github/workflows/skills-catalog.yml` together.

Canonical operations for humans remain:

```bash
npx skills add zozo123/ariflow-swfactory --list
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory -y
npx skills add zozo123/ariflow-swfactory --skill swfactory -y
npx skills add vercel-labs/skills@find-skills -y
npx skills find airflow --owner vercel-labs
```

For Python callers, use `SWFACTORY_SKILL`, `SWFACTORY_HARNESS_SKILL`, `VERCEL_FIND_SKILLS`, `find_argv`, `install_argv`, and `list_argv` from `swfactory.skills_connector`. The connector's `catalog` command reports the exact CLI version and both public catalog URLs so deployment evidence can record what source was intended.

## Distribution contract

Keep public skills at `skills/<slug>/SKILL.md`. Keep each skill's supporting files inside its own directory so skills.sh snapshots remain scoped to that skill rather than the whole repository. Keep the root `skills.sh.json` synchronized with the published skill slugs. Every public skill also carries `agents/openai.yaml`; the normal pytest suite enforces the manifest, frontmatter and ChatGPT metadata together.

The repository also carries `.claude-plugin/plugin.json` for plugin-aware hosts. That manifest is metadata only; skill discovery remains driven by the `SKILL.md` packages.

## Indexing verification

A valid skill can be installable before it appears in the skills.sh search/catalog. Verify distribution and indexing separately:

1. `npx skills add zozo123/ariflow-swfactory --list` must discover both public skills.
2. A clean isolated install of each skill must succeed.
3. Repository CI must pass the pinned local discovery check; after merge, the push workflow also lists the skills through the public GitHub source.
4. Search `skills.sh` for `zozo123/ariflow-swfactory` and each canonical skill slug.
5. If CLI discovery succeeds but the public catalog is absent, open an indexing request in `vercel-labs/skills` with the repository URL, skill paths, install commands, `skills.sh.json`, and observed catalog state.

Do not generate fake installs or telemetry to influence ranking. Catalog rank should reflect real usage.
