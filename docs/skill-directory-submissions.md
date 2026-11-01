# Skill directory submissions

Owner-authorized listing pack for
[`airflow-software-factory`](../skills/airflow-software-factory/SKILL.md)
(`https://www.skillsdirectory.com/skills/zozo123-airflow-software-factory`).

This cloud agent could open the in-repo skill PR, but the GitHub App token cannot
fork third-party repositories or replace repo topics. Use the ready-to-submit
entries below from an account that can fork and open PRs (Yossi / zozo123).

Install command for reviewers:

```bash
npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

Skill path:
`https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory`

## Already listed

| Directory | Status |
| --- | --- |
| [skills.sh](https://skills.sh/zozo123/ariflow-swfactory/airflow-software-factory) | Indexed |
| [Skills Directory](https://www.skillsdirectory.com/skills/zozo123-airflow-software-factory) | Grade A listing |

## Ready-to-submit GitHub PRs

Submit **one PR per list**. Skip any list that later adds a no-self-promotion or
no-AI-PR rule. Do not touch islo-labs or Incredibuild-RND orgs.

### 1. ComposioHQ/awesome-claude-skills

- List: https://github.com/ComposioHQ/awesome-claude-skills
- File: `README.md`
- Section: `### Development & Code Tools`
- Placement: alphabetical, **before** `artifacts-builder` (first entry in the section)
- Exact line:

```markdown
- [Airflow Software Factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) - Operate an Apache Airflow software factory that turns GitHub issues into tested, reviewed PRs with human gates and evidence. *By [@zozo123](https://github.com/zozo123)*
```

- PR title: `Add Airflow Software Factory skill`
- PR body:

```text
Adds the public airflow-software-factory Agent Skill from zozo123/ariflow-swfactory
to Development and Code Tools.

Problem: coding agents need a governed Airflow issue-to-PR workflow with gates
and evidence, not ad-hoc patch loops.

Who uses it: maintainers adopting or operating SWFactory.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory

Attribution: authored by @zozo123.
```

### 2. VoltAgent/awesome-agent-skills

- List: https://github.com/VoltAgent/awesome-agent-skills
- File: `README.md`
- Section: Community Skills → Development and Testing (`</details>` closes the block)
- Placement: **append at end** of that `<details>` list (CONTRIBUTING rule)
- Exact line (description ≤10 words):

```markdown
- **[zozo123/airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory)** - Airflow-governed issue-to-PR software factory
```

- PR title: `Add skill: zozo123/airflow-software-factory`
- PR body:

```text
Public Agent Skill for SWFactory, already listed on skills.sh and Skills Directory
(Grade A). Appends one Community Skills / Development and Testing entry.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

Note: CONTRIBUTING asks for demonstrated community usage. Cite the skills.sh and
Skills Directory listings if maintainers ask.

### 3. BehiSecc/awesome-claude-skills

- List: https://github.com/BehiSecc/awesome-claude-skills
- File: `README.md`
- Section: `## Development & Code Tools`
- Placement: alphabetical by name, after `agenttrace-session-audit`, before `Archcore skills`
- Exact line:

```markdown
- [airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) - Operate an Apache Airflow software factory for coding agents: GitHub issues to tested, reviewed PRs with human gates and evidence.
```

- PR title: `Add airflow-software-factory skill`
- PR body:

```text
Adds one Development and Code Tools entry for the public SWFactory Agent Skill.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

### 4. jghoman/awesome-apache-airflow

- List: https://github.com/jghoman/awesome-apache-airflow
- File: `README.md`
- Section: `## Libraries, Hooks, Utilities`
- Placement: **bottom of the category** (CONTRIBUTING rule)
- Exact line:

```markdown
- [SWFactory](https://github.com/zozo123/ariflow-swfactory) - Airflow-governed software factory that turns GitHub issues into tested, reviewed pull requests with human approval gates, Factory Cells, and agent skills.
```

- PR title: `Add SWFactory to Libraries, Hooks, Utilities`
- PR body:

```text
Adds SWFactory under Libraries, Hooks, Utilities.

Repository: https://github.com/zozo123/ariflow-swfactory

It uses Apache Airflow as the sole lifecycle scheduler for agentic issue-to-PR
delivery with approval gates and retained evidence.
```

### 5. truefoundry/awesome-skills-registry

- List: https://github.com/truefoundry/awesome-skills-registry
- File: `sources.yaml`
- Placement: with other `skills_path: skills/` sources (automated scanner)
- Exact lines:

```yaml
  - url: https://github.com/zozo123/ariflow-swfactory
    skills_path: skills/
```

- PR title: `Add zozo123/ariflow-swfactory skills source`
- PR body:

```text
Registers the public SWFactory skill packages so the registry scanner can index
skills/airflow-software-factory and skills/swfactory.

Source: https://github.com/zozo123/ariflow-swfactory
```

### 6. skillmatic-ai/awesome-agent-skills

- List: https://github.com/skillmatic-ai/awesome-agent-skills
- File: `README.md`
- Section: `### Skill Marketplaces & Directories` (or `#### Popular Collections` if they prefer project links)
- Placement: end of Skill Marketplaces & Directories is fine for a directory-style project link; for the skill repo itself prefer Popular Collections
- Exact line (Popular Collections):

```markdown
- [zozo123/ariflow-swfactory](https://github.com/zozo123/ariflow-swfactory) - Airflow software factory Agent Skills for governed issue-to-PR delivery.
```

- PR title: `Add SWFactory Agent Skills`
- PR body:

```text
Adds the SWFactory repository under Popular Collections.

It publishes Agent Skills compatible SKILL.md packages for adopting and operating
an Airflow-governed coding factory.
```

### 7. msantino/awesome-airflow

- List: https://github.com/msantino/awesome-airflow
- File: `README.md`
- Section: `### Open Source Projects using Apache Airflow`
- Placement: end of that short list
- Exact line:

```markdown
* [SWFactory](https://github.com/zozo123/ariflow-swfactory)
```

- PR title: `Add SWFactory open source project`
- PR body:

```text
Adds SWFactory to Open Source Projects using Apache Airflow.

https://github.com/zozo123/ariflow-swfactory
```

### 8. karanb192/awesome-claude-skills

- List: https://github.com/karanb192/awesome-claude-skills
- File: `README.md`
- Section: `### Development & Architecture`
- Placement: alphabetical by skill name, after `api-designer`, before `aria-icons`
- Exact line:

```markdown
| [airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) | Adopt and operate an Airflow software factory that turns GitHub issues into tested, reviewed PRs. |
```

- PR title: `Add airflow-software-factory to Development & Architecture`
- PR body:

```text
Adds one Development and Architecture table row for the public SWFactory skill.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

### 9. kodustech/awesome-agent-skills

- List: https://github.com/kodustech/awesome-agent-skills
- File: `README.md`
- Section: `### DevOps`
- Placement: alphabetical by skill name among table rows (`airflow-software-factory` near the top of the DevOps table)
- Exact line:

```markdown
| [airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) | Operate an Apache Airflow software factory that turns GitHub issues into tested, reviewed pull requests with human gates and evidence. Use when adopting, configuring, running, or auditing SWFactory. |
```

- PR title: `Add airflow-software-factory to DevOps`
- PR body:

```text
Adds the SWFactory Agent Skill under DevOps.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

### 10. Chat2AnyLLM/awesome-claude-skills

- List: https://github.com/Chat2AnyLLM/awesome-claude-skills
- File: `README.md` (and site source if the repo mirrors it)
- Placement: Development / code tools style section if present; otherwise a Community skills bullet at the end of the closest matching category
- Exact line:

```markdown
- [airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) - Airflow software factory skill for governed GitHub issue-to-PR delivery.
```

- PR title: `Add airflow-software-factory`
- PR body:

```text
Adds the public SWFactory Agent Skill.

https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory
```

### 11. Bayzid03/awesome-airflow

- List: https://github.com/Bayzid03/awesome-airflow
- File: `README.md`
- Placement: Projects / Tools section if present; otherwise Resources
- Exact line:

```markdown
- [SWFactory](https://github.com/zozo123/ariflow-swfactory) - Airflow-governed software factory for agentic issue-to-PR delivery with approval gates.
```

- PR title: `Add SWFactory`
- PR body:

```text
Adds SWFactory as an Airflow-related open source project.

https://github.com/zozo123/ariflow-swfactory
```

### 12. futantan/agent-skills.md

- List: https://github.com/futantan/agent-skills.md
- File: follow the repo's current README / catalog format
- Exact line (link style, adjust if the README uses a table):

```markdown
- [airflow-software-factory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/airflow-software-factory) - Airflow-governed software factory Agent Skill.
```

- PR title: `Add airflow-software-factory`
- PR body:

```text
Adds SWFactory's public Agent Skill to the catalog.

Install: npx skills add zozo123/ariflow-swfactory --skill airflow-software-factory
```

## Skipped (policy or threshold)

| List | Why skipped for now |
| --- | --- |
| travisvn/awesome-claude-skills | CONTRIBUTING forbids promotional submissions |
| JackyST0/awesome-agent-skills | Requires ≥256 stars for community projects (repo has 2) |
| heilcheng/awesome-agent-skills | Requires ≥20 stars and age >14 days for automated checks (stars short) |
| anthropics/skills | Official Anthropic examples repo; not a community listing target |
| islo-labs / Incredibuild-RND | Explicitly out of scope |

Revisit heilcheng once the repo has ≥20 stars; revisit JackyST0 at ≥256.

## Signup-only / web forms (owner submit)

These need a browser login or publisher dashboard rather than a GitHub PR from this agent:

| Site | Action for owner |
| --- | --- |
| https://www.skillsdirectory.com/skills/zozo123-airflow-software-factory | Already listed; claim author badge / live security badge if desired |
| https://skills.sh/zozo123/ariflow-swfactory | Already indexed; no PR needed |
| https://skillsmp.com/ | Web marketplace signup / submit flow |
| https://agentskill.sh | Directory signup / submit if they accept publisher listings |
| https://claudeskills.info/ | Web directory submission if available |
| https://claudebin.com | Web listing / submit |
| https://officialskills.sh | VoltAgent companion site; listing usually follows the awesome-agent-skills PR |
| https://ai-skills.io | kodustech companion; usually follows their awesome list PR |
| Claude Code plugin marketplace | Publish `.claude-plugin/plugin.json` via Anthropic/plugin host flow if desired |
| GitHub repo topics | Token lacked `Administration` scope. Desired topics: `airflow`, `agent-skills`, `claude-skills`, `ai-agents`, `software-factory`, `llm`, `devtools` (keep existing topics too) |

## Companion skill (optional second entry)

Where a list accepts multiple skills from one repo, also offer:

```markdown
- [swfactory](https://github.com/zozo123/ariflow-swfactory/tree/main/skills/swfactory) - Drive SWFactory from an outer coding harness through the swf CLI with stable session identity.
```

Keep that as a **separate** PR only if the target list's CONTRIBUTING allows a second entry after the first merges.
