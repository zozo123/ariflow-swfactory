"""Every module must be reachable from something that actually runs, or be declared debt.

The repository had no answer to "does anything call this?", and the cost was not theoretical:
`swfactory.execution_binding` was named in `config/capability-inventory.json` as the runtime entry
for parallel workgraph while being imported by nothing at all, and `swfactory.smolvm_backend`
posted a create route no daemon serves -- neither is visible to a test suite that only asks whether
the code it imports behaves.

This is the missing question, asked once. A module is reachable when an import path leads to it
from a declared entrypoint: the console script, a `python -m` target, a packaging entry point, a
DAG, or a repository script. Anything else is debt, and debt has to be written down.

`NOT_YET_WIRED` is compared for EQUALITY, not containment. A new orphan fails, and so does wiring
one up without striking it off -- a ledger that only ever grows is a ledger nobody reads.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "swfactory" if (ROOT / "swfactory").is_dir() else ROOT / "src" / "swfactory"

# Declared in pyproject.toml: `[project.scripts]` and `[project.entry-points]`, plus every
# `python -m swfactory.<module>` the repository tells an operator to run.
ENTRYPOINTS = frozenset(
    {
        "cli",
        "astronomer_blueprint",
        "candidate_readiness",
        "capability_inventory",
        "evals",
        "lifecycle",
        "liquid_spec",
        "skills_connector",
    }
)

# Debt, with the reason it is not wired yet. Strike an entry off the moment something imports it.
# Debt, with the reason it is not wired yet. It lives in ``config/not-yet-wired.json`` so this gate
# and ``swfactory improve`` read one file: a ledger the loop cannot see is a ledger it cannot retire.
LEDGER = ROOT / "config" / "not-yet-wired.json"
NOT_YET_WIRED: dict[str, str] = json.loads(LEDGER.read_text(encoding="utf-8"))["modules"]


def imports_of(tree: ast.AST, package: str = "") -> set[str]:
    """Every swfactory module this file imports, relative imports resolved.

    Relative imports have to be resolved against the importing file's own package, and a package's
    ``__init__`` resolves against itself -- miss that and `swfactory.backend.service`, 1500 lines
    reached through `from .service import Factory`, reads as unreachable.
    """
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = package if node.level == 1 else ""
                found |= (
                    {f"{base}.{node.module}".lstrip(".")}
                    if node.module
                    else {f"{base}.{alias.name}".lstrip(".") for alias in node.names}
                )
            elif node.module and node.module.startswith("swfactory"):
                tail = node.module.removeprefix("swfactory").lstrip(".")
                found |= {tail} if tail else {alias.name for alias in node.names}
        elif isinstance(node, ast.Import):
            found |= {a.name.removeprefix("swfactory.") for a in node.names if a.name.startswith("swfactory.")}
    return found


def import_graph() -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for path in SRC.rglob("*.py"):
        if "prompts" in path.parts:
            continue
        module = path.relative_to(SRC).with_suffix("").as_posix().replace("/", ".").removesuffix(".__init__")
        package = module if path.name == "__init__.py" else (module.rsplit(".", 1)[0] if "." in module else "")
        graph[module] = imports_of(ast.parse(path.read_text(encoding="utf-8")), package)
    return graph


def reachable(graph: dict[str, set[str]]) -> set[str]:
    roots = set(ENTRYPOINTS)
    for driver in sorted((ROOT / "dags").glob("*.py")) + sorted((ROOT / "scripts").glob("*.py")):
        roots |= imports_of(ast.parse(driver.read_text(encoding="utf-8")))
    seen: set[str] = set()
    stack = list(roots)
    while stack:
        module = stack.pop()
        if module in seen:
            continue
        seen.add(module)
        stack.extend(graph.get(module, ()))
        if "." in module:  # a submodule drags in its package __init__
            stack.append(module.split(".")[0])
    return seen


def unreachable_modules() -> set[str]:
    graph = import_graph()
    return {module for module in graph if module and module != "__init__"} - reachable(graph)


def test_every_module_is_reachable_or_written_down() -> None:
    found = unreachable_modules()
    ledgered = set(NOT_YET_WIRED)

    assert found - ledgered == set(), f"unreachable and undeclared: {sorted(found - ledgered)}"
    assert ledgered - found == set(), f"now reachable, strike from NOT_YET_WIRED: {sorted(ledgered - found)}"


def test_the_declared_entrypoints_all_exist() -> None:
    graph = import_graph()

    assert set(graph) >= ENTRYPOINTS, f"declared entrypoint has no module: {sorted(ENTRYPOINTS - set(graph))}"


def test_a_package_init_resolves_its_own_relative_imports() -> None:
    """`swfactory.backend.service` is reached only through `from .service import Factory` in the
    package `__init__`. Resolving that against the parent instead of the package itself reported
    1500 reachable lines as dead."""
    tree = ast.parse("from .service import Factory\nfrom .server import serve\n")

    assert imports_of(tree, "backend") == {"backend.service", "backend.server"}


def test_a_new_orphan_is_caught() -> None:
    graph = {"cli": {"used"}, "used": set(), "stranded": set()}

    assert "stranded" in ({m for m in graph} - reachable_from(graph, {"cli"}))


def reachable_from(graph: dict[str, set[str]], roots: set[str]) -> set[str]:
    seen: set[str] = set()
    stack = list(roots)
    while stack:
        module = stack.pop()
        if module in seen:
            continue
        seen.add(module)
        stack.extend(graph.get(module, ()))
    return seen
