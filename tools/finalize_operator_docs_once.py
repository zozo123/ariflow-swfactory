from pathlib import Path

path = Path("docs/swf.md")
text = path.read_text()
replacements = {
    "FILTERS_GATES": "`--dag`, `--run`, `--issue`, `--state`, `--actor`, `--limit`",
    "FILTERS_JOBS": "`--dag`, `--run`, `--issue`, `--state`, `--attention`, `--limit`",
    "FILTERS_RUNS": "`--dag`, `--state`, `--since`, `--limit`",
    "EXAMPLES_BLOCK": """```sh
swf gates list --dag factory --state deferred --limit 50
swf jobs list --issue 2034 --attention --limit 25
swf runs list --dag factory --state running --limit 20
```

Listings never widen when another filter is added. If a source is truncated or unavailable, the
result reports that explicitly; an empty selection is never used as a substitute for a failed read.""",
    "DRYRUN_PARA": """Bulk gate commands use the same filters as `gates list`. `--dry-run` performs the full
selection and readiness checks but sends no PATCH request. The output names every selected gate,
its current readiness and anything skipped because it is arming, stale, already answered or
outside the bounded `--limit`.

```sh
swf gates approve --all --dag factory --state deferred --limit 25 --dry-run
swf gates approve --all --dag factory --state deferred --limit 25 --yes
```""",
}
for old, new in replacements.items():
    if old not in text:
        raise SystemExit(f"expected placeholder missing: {old}")
    text = text.replace(old, new)
path.write_text(text)
