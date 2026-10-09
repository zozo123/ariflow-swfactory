"""Suite-wide fixtures: hermetic git for every test, the one-commit ``repo`` the git tests share, and
the backend harness over ``backend_support``.

Shared helpers that are not fixtures live in ``tests/support.py`` and ``tests/backend_support.py``.
"""

from __future__ import annotations

import threading
import types
from collections.abc import Callable, Iterator
from contextlib import closing
from pathlib import Path

import pytest
from backend_support import AF, REPO_SLUG, SUBMIT_ROUTES, TOKEN, Backend, Client, FakeAirflow, write_line
from support import make_repo

from swfactory.admission import Limits
from swfactory.backend import Factory, make_server
from swfactory.control import AirflowClient


@pytest.fixture(scope="session", autouse=True)
def _hermetic_git(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """No user/system git config (signing, hooks templates, credential helpers) reaches any git the
    suite runs, its module-scoped pipeline runs and DAG subprocesses included. It carries no
    identity, so a swfactory commit that drops its own ``-c user.name=...`` fails; the commits
    tests make carry ``support.IDENT``."""
    config = tmp_path_factory.mktemp("git") / "gitconfig"
    config.write_text("[commit]\n\tgpgsign = false\n[init]\n\tdefaultBranch = main\n", encoding="utf-8")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("GIT_CONFIG_GLOBAL", str(config))
        mp.setenv("GIT_CONFIG_NOSYSTEM", "1")
        mp.setenv("GIT_TERMINAL_PROMPT", "0")
        yield


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return make_repo(tmp_path)[0]


# ---------------------------------------------------------------- backend


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ambient credentials/flags decide backend behavior; pin them so the suite is order-free."""
    for name in ("AIRFLOW_TOKEN", "AIRFLOW_USER", "AIRFLOW_PASSWORD", "SWF_DRAIN", "SWF_GENERATION"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def airflow() -> FakeAirflow:
    return FakeAirflow(SUBMIT_ROUTES)


@pytest.fixture
def factory(tmp_path: Path, airflow: FakeAirflow, env: None) -> Iterator[Factory]:
    """A backend whose Airflow is ``airflow``, closed after the test."""
    with closing(
        Factory(
            token=TOKEN,
            airflow_url=AF,
            repo=REPO_SLUG,
            owner="operator",
            root=tmp_path / "metrics",
            state_root=tmp_path / "state",
        )
    ) as made:
        # Replace the transport *after* construction, the way test_control fakes an opener: the
        # real one is a urllib opener that would dial 127.0.0.1:8080 on the first read route.
        made.opener = types.SimpleNamespace(open=airflow.open)  # type: ignore[assignment]
        made.credentials = AirflowClient(AF, token="airflow-token", opener=airflow.open)
        yield made


@pytest.fixture
def client(factory: Factory, airflow: FakeAirflow) -> Iterator[Client]:
    """``factory`` served by the real ``make_server`` on an ephemeral loopback port."""
    server = make_server(factory, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield Client(server, airflow)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Backend]:
    """Builds a restartable :class:`Backend` whose blueprint lives in the cwd, since that is how
    blueprints resolve."""

    def build(limits: Limits, *extra: str) -> Backend:
        write_line(tmp_path, *extra)
        monkeypatch.chdir(tmp_path)
        return Backend(tmp_path, limits)

    return build
