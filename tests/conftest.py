"""Suite-wide fixtures: hermetic git for every test, and the one-commit ``repo`` the git tests share.

Shared helpers that are not fixtures live in ``tests/support.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from support import make_repo


@pytest.fixture(scope="session", autouse=True)
def _hermetic_git(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """No user/system git config (signing, hooks templates, credential helpers) reaches any git the
    suite runs, its module-scoped pipeline runs and DAG subprocesses included. The identity sits at
    global precedence, so the ``-c user.name=swfactory-bot`` that swfactory passes still wins."""
    config = tmp_path_factory.mktemp("git") / "gitconfig"
    config.write_text(
        "[user]\n\tname = t\n\temail = t@t\n[commit]\n\tgpgsign = false\n[init]\n\tdefaultBranch = main\n",
        encoding="utf-8",
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("GIT_CONFIG_GLOBAL", str(config))
        mp.setenv("GIT_CONFIG_NOSYSTEM", "1")
        mp.setenv("GIT_TERMINAL_PROMPT", "0")
        yield


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return make_repo(tmp_path)[0]
