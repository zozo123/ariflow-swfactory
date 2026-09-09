"""GitHub -> managed work-order webhook receiver for the orchestrator sandbox. Stdlib only.

The factory itself runs on islo: one long-lived ``swf-orchestrator`` sandbox hosts
``airflow standalone`` and this receiver on port 8081. GitHub posts to an islo incoming webhook
(``islo webhook incoming create --deliver-to-port 8081 --path /webhooks/github ...``), islo
verifies the HMAC and de-duplicates on ``X-GitHub-Delivery``, and delivers the request to this
process, which durably queues the event before replying. A background dispatcher then submits one
work order per delivery to the backend's managed admission boundary::

    issues.labeled  label "factory"         -> POST /v1/work-orders {"line": "factory", ...}
    issues.labeled  label "factory:<name>"  -> POST /v1/work-orders {"line": "<name>", ...}
    issue_comment.created "@factory run [<name>]" on an issue -> same as above
    pull_request.*, factory:blocked / factory:rejected (deliver's PR labels), else -> ignored

The backend owns admission: it reserves capacity, activates the Factory Cells and hands Airflow the
run with its ``_factory_cells`` bindings. Talking to Airflow from here instead (``work_orders=None``)
is LEGACY: those runs are unmanaged, and nothing fences a second one against them. Airflow remains
the only lifecycle scheduler either way -- this receiver decides admission, never timing.

``route`` is pure and unit-tested; ``verify_signature`` implements GitHub's ``sha256=`` scheme
for the case where the receiver is exposed without islo in front (``--secret-env``); the HTTP
calls take an ``opener`` so tests never need a real Airflow. Endpoints verified against the
installed apache-airflow 3.3.1 (``POST /auth/token`` -> ``{"access_token"}``, simple auth
manager; ``POST /api/v2/dags/{dag_id}/dagRuns`` with ``TriggerDAGRunPostBody``).
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import ipaddress
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from typing import Any

from swfactory.dispatch import DeliveryConflict, DeliveryInbox, Dispatcher, InboxFull
from swfactory.paths import validate_repo

DEFAULT_DAG = "factory"
LABEL = "factory"
COMMENT_COMMAND = "@factory run"
WEBHOOK_PATH = "/webhooks/github"
HEALTH_PATH = "/healthz"
READY_PATH = "/readyz"
WORK_ORDERS_PATH = "/v1/work-orders"
# Every GitHub-shaped intake submits under one actor so that the same label, arriving twice through
# two channels, hashes to the *same* work order instead of racing two admissions at one Cell.
MANAGED_ACTOR = "github"
SIGNATURE_HEADER = "X-Hub-Signature-256"
SIGNATURE_PREFIX = "sha256="
# Labels ``deliver`` writes on PRs; a labeled event carrying one is never a dispatch.
STATUS_LABELS = frozenset({"factory:blocked", "factory:rejected"})
# Same rule as ``Blueprint.name`` (= DAG id); anything else cannot be a blueprint.
_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")
_HTTP_TIMEOUT_S = 30
MAX_BODY_BYTES = 1_048_576
TRUSTED_ASSOCIATIONS = frozenset({"OWNER", "MEMBER", "COLLABORATOR"})

Opener = Callable[..., Any]
TokenProvider = Callable[[], str]


@dataclass(frozen=True)
class Trigger:
    """One Airflow DAG run to create: ``POST /api/v2/dags/{dag_id}/dagRuns`` with ``conf``."""

    dag_id: str
    conf: dict[str, Any] = field(default_factory=dict)
    dag_run_id: str | None = None

    def body(self) -> dict[str, Any]:
        """The ``TriggerDAGRunPostBody`` (``logical_date`` is required, ``null`` = now)."""
        body: dict[str, Any] = {"conf": self.conf, "logical_date": None}
        if self.dag_run_id is not None:
            body["dag_run_id"] = self.dag_run_id
        return body


@dataclass(frozen=True)
class WorkOrders:
    """The managed admission boundary an intake submits through.

    Present = managed mode: the receiver never touches Airflow, so a run can never exist without
    the backend's durable admission, capacity accounting and Factory Cell bindings behind it.
    Absent = legacy direct-Airflow mode, kept only for a sandbox with no backend in front of it.
    """

    url: str
    token: str
    actor: str = MANAGED_ACTOR


# ---------------------------------------------------------------- routing (pure)


def route(event: str, payload: Mapping[str, Any]) -> Trigger | None:
    """Map a GitHub event to a ``Trigger`` or ``None`` (ignore).

    ``event`` is the ``X-GitHub-Event`` header value. Only ``issues`` (action ``labeled`` with
    a ``factory`` / ``factory:<name>`` label) and ``issue_comment`` (action ``created``, body
    starting with ``@factory run [<name>]``) from a trusted repository association on real issues
    dispatch. Comments and labels on pull requests (``issue.pull_request`` present), untrusted
    comments, ``pull_request`` events and malformed payloads yield ``None``. Never raises on
    payload shape.
    """
    if not isinstance(payload, Mapping):
        return None
    issue = payload.get("issue")
    if not isinstance(issue, Mapping) or "pull_request" in issue:
        return None
    number = issue.get("number")
    if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
        return None
    action = payload.get("action")
    if event == "issues" and action == "labeled":
        label = payload.get("label")
        name = label.get("name") if isinstance(label, Mapping) else None
        dag_id = _dag_from_label(name)
    elif event == "issue_comment" and action == "created":
        comment = payload.get("comment")
        association = comment.get("author_association") if isinstance(comment, Mapping) else None
        if not isinstance(association, str) or association not in TRUSTED_ASSOCIATIONS:
            return None
        body = comment.get("body") if isinstance(comment, Mapping) else None
        dag_id = _dag_from_comment(body)
    else:
        return None
    if dag_id is None:
        return None
    return Trigger(dag_id=dag_id, conf={"issues": [str(number)]})


def _dag_from_label(name: object) -> str | None:
    """``factory`` -> ``factory``; ``factory:<name>`` -> ``<name>``; anything else -> None."""
    if not isinstance(name, str) or name in STATUS_LABELS:
        return None
    if name == LABEL:
        return DEFAULT_DAG
    if name.startswith(f"{LABEL}:"):
        return _valid_name(name[len(LABEL) + 1 :])
    return None


def _dag_from_comment(body: object) -> str | None:
    """First line ``@factory run`` -> ``factory``; ``@factory run <name>`` -> ``<name>``."""
    if not isinstance(body, str):
        return None
    first = body.strip().splitlines()[0].strip() if body.strip() else ""
    if not first.startswith(COMMENT_COMMAND):
        return None
    rest = first[len(COMMENT_COMMAND) :]
    if rest and not rest[0].isspace():
        return None  # "@factory runner" is not a command
    words = rest.split()
    if not words:
        return DEFAULT_DAG
    if len(words) > 1:
        return None
    return _valid_name(words[0])


def _valid_name(name: str) -> str | None:
    return name if _NAME_RE.fullmatch(name) else None


def _repository(payload: Mapping[str, Any]) -> str:
    repository = payload.get("repository")
    name = repository.get("full_name") if isinstance(repository, Mapping) else None
    if not isinstance(name, str):
        raise ValueError("routed deliveries require repository.full_name")
    return validate_repo(name).casefold()


def repository_trigger(trigger: Trigger, repository: str) -> Trigger:
    """An issue number belongs to one repository; validate against the locally installed route."""
    from swfactory.blueprint import load

    repository = validate_repo(repository).casefold()
    blueprint = load(trigger.dag_id)
    if blueprint.name != trigger.dag_id:
        raise ValueError("blueprint name does not match the requested DAG")
    targets = list(dict.fromkeys(target.repo for target in blueprint.targets if target.repo.casefold() == repository))
    if not targets:
        raise ValueError("webhook repository is not a target of this blueprint")
    return Trigger(trigger.dag_id, {**trigger.conf, "targets": targets})


# ---------------------------------------------------------------- signature


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """GitHub's ``X-Hub-Signature-256``: ``sha256=<hex hmac-sha256(secret, body)>``.

    Constant-time comparison; a missing or malformed header is ``False``. Optional in
    production because the islo incoming webhook verifies the same HMAC upstream.
    """
    if not header or not header.startswith(SIGNATURE_PREFIX):
        return False
    supplied = header[len(SIGNATURE_PREFIX) :].strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", supplied):
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, supplied)


# ---------------------------------------------------------------- Airflow REST calls


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Reject redirects so credentials cannot cross origins through urllib."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirect()).open


def _safe_base(url: str, *, what: str, allow_var: str, env: Mapping[str, str] | None = None) -> str:
    """Require HTTPS, except for loopback or an explicitly named internal HTTP host.

    The backend bearer token buys the same mutations an Airflow token does, so it is held to the
    same rule: a credential never leaves this process over plaintext to a host nobody named.
    """

    env = os.environ if env is None else env
    parsed = urllib.parse.urlsplit(url)
    host = (parsed.hostname or "").lower()
    try:
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
        _ = parsed.port
    except ValueError:
        loopback = False
        if host:
            try:
                _ = parsed.port
            except ValueError as error:
                raise ValueError(f"{what} URL has an invalid port") from error
    allowed_http = {item.strip().lower() for item in env.get(allow_var, "").split(",") if item.strip()}
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"{what} URL must not contain credentials, query, or fragment")
    if parsed.scheme == "https" and host:
        return url.rstrip("/")
    if parsed.scheme == "http" and (loopback or host in allowed_http):
        return url.rstrip("/")
    raise ValueError(f"{what} URL must use HTTPS; HTTP hosts require {allow_var}")


def _safe_airflow_base(url: str, env: Mapping[str, str] | None = None) -> str:
    return _safe_base(url, what="Airflow", allow_var="SWF_AIRFLOW_HTTP_HOSTS", env=env)


def _safe_backend_base(url: str, env: Mapping[str, str] | None = None) -> str:
    return _safe_base(url, what="Backend", allow_var="SWF_BACKEND_HTTP_HOSTS", env=env)


def _post_json(url: str, payload: Mapping[str, Any], *, headers: Mapping[str, str], opener: Opener) -> tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json", **headers},
        method="POST",
    )
    with opener(req, timeout=_HTTP_TIMEOUT_S) as resp:
        return int(getattr(resp, "status", 200)), resp.read().decode("utf-8", errors="replace")


def airflow_token(url: str, username: str, password: str, opener: Opener = _NO_REDIRECT_OPENER) -> str:
    """``POST {url}/auth/token {"username", "password"}`` -> the JWT ``access_token``.

    Airflow 3.3.1 simple auth manager; the generated admin password lives in
    ``$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated``.
    """
    base = _safe_airflow_base(url)
    _, text = _post_json(
        f"{base}/auth/token",
        {"username": username, "password": password},
        headers={},
        opener=opener,
    )
    try:
        token = json.loads(text)["access_token"]
    except (ValueError, KeyError, TypeError) as e:
        raise RuntimeError(f"unexpected /auth/token response: {text[:200]}") from e
    if not isinstance(token, str) or not token:
        raise RuntimeError("empty access_token from /auth/token")
    return token


def trigger_airflow(trigger: Trigger, *, airflow_url: str, token: str, opener: Opener = _NO_REDIRECT_OPENER) -> str:
    """``POST {airflow_url}/api/v2/dags/{dag_id}/dagRuns`` with ``trigger.body()`` and a Bearer
    token. An identified trigger must return its exact run identity and configuration. After a
    409, GET that run and compare its evidence; a conflict alone is never a successful receipt.
    Unidentified callers retain their synchronous response contract."""
    base = _safe_airflow_base(airflow_url)
    if _valid_name(trigger.dag_id) is None:
        raise ValueError("invalid webhook DAG id")
    url = f"{base}/api/v2/dags/{urllib.parse.quote(trigger.dag_id, safe='')}/dagRuns"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        _, text = _post_json(url, trigger.body(), headers=headers, opener=opener)
    except urllib.error.HTTPError as exc:
        if exc.code != 409 or trigger.dag_run_id is None:
            raise
        exc.close()
        request = urllib.request.Request(
            f"{url}/{urllib.parse.quote(trigger.dag_run_id, safe='')}",
            headers={"Accept": "application/json", **headers},
            method="GET",
        )
        try:
            with opener(request, timeout=_HTTP_TIMEOUT_S) as response:
                text = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as read_error:
            if read_error.code != 404:
                raise
            read_error.close()
            raise RuntimeError("conflicting Airflow run is not yet readable") from None
    try:
        data = json.loads(text)
    except ValueError:
        if trigger.dag_run_id is not None:
            raise RuntimeError("Airflow returned an invalid run receipt") from None
        return text.strip()
    if trigger.dag_run_id is not None:
        if not isinstance(data, Mapping) or not all(key in data for key in ("dag_id", "dag_run_id", "conf")):
            raise RuntimeError("Airflow returned an incomplete run receipt")
        if data["dag_id"] != trigger.dag_id or data["dag_run_id"] != trigger.dag_run_id or data["conf"] != trigger.conf:
            raise DeliveryConflict("Airflow run identity or configuration differs from delivery")
        return trigger.dag_run_id
    run_id = data.get("dag_run_id") if isinstance(data, Mapping) else None
    return str(run_id) if run_id else text.strip()


def submit_work_order(
    trigger: Trigger,
    backend: WorkOrders,
    *,
    opener: Opener = _NO_REDIRECT_OPENER,
) -> dict[str, Any]:
    """``POST {backend.url}/v1/work-orders`` -- the one managed admission boundary.

    The backend owns everything past this call: whether the order is admitted or queued, which
    Factory Cells it binds, and whether an Airflow run is created at all. The receiver deliberately
    learns none of that; it only needs a durable receipt naming the work order, so a redelivery can
    be recognised as the same admission instead of becoming a second one. Refusals arrive as
    ``HTTPError`` and stay refusals -- there is no Airflow fallback to launder them into a run.
    """
    base = _safe_backend_base(backend.url)
    if _valid_name(trigger.dag_id) is None:
        raise ValueError("invalid webhook line name")
    # Only the fields the canonical route admits on. The inbox's own ``_swfactory_webhook`` block is
    # receiver bookkeeping; sending it would change the request digest and split the work identity
    # a second channel has to collide with.
    order: dict[str, Any] = {"line": trigger.dag_id, "actor": backend.actor}
    for key in ("issues", "targets"):
        value = trigger.conf.get(key)
        if value:
            order[key] = list(value)
    _, text = _post_json(
        base + WORK_ORDERS_PATH,
        order,
        headers={"Authorization": f"Bearer {backend.token}"},
        opener=opener,
    )
    try:
        document = json.loads(text)
    except ValueError:
        raise RuntimeError("backend returned an invalid work-order receipt") from None
    if not isinstance(document, Mapping) or not isinstance(document.get("submission_id"), str):
        raise RuntimeError("backend returned an incomplete work-order receipt")
    return dict(document)


def token_provider_from_env(
    airflow_url: str,
    env: Mapping[str, str] | None = None,
    opener: Opener = _NO_REDIRECT_OPENER,
) -> TokenProvider:
    """``AIRFLOW_TOKEN`` (static JWT) or ``AIRFLOW_USER`` + ``AIRFLOW_PASSWORD`` (a fresh login
    per event, so the JWT's expiry never matters). Raises ``ValueError`` when neither is set."""
    env = os.environ if env is None else env
    token = env.get("AIRFLOW_TOKEN")
    if token:
        return lambda: token
    user, password = env.get("AIRFLOW_USER"), env.get("AIRFLOW_PASSWORD")
    if user and password:
        return lambda: airflow_token(airflow_url, user, password, opener)
    raise ValueError("set AIRFLOW_TOKEN, or AIRFLOW_USER and AIRFLOW_PASSWORD")


# ---------------------------------------------------------------- HTTP server


def make_handler(
    *,
    airflow_url: str,
    token_provider: TokenProvider | None,
    secret: str | None = None,
    opener: Opener = _NO_REDIRECT_OPENER,
    log: Callable[[str], None] | None = None,
    inbox: DeliveryInbox | None = None,
    dispatcher: Dispatcher | None = None,
    work_orders: WorkOrders | None = None,
) -> type[BaseHTTPRequestHandler]:
    """Build the request handler class bound to one submission boundary and one credential.

    ``POST /webhooks/github`` -> 202 when the event is admitted, 200 ``{"routed": false}`` when it
    is ignored, 400 on bad JSON, 401 on a bad signature (only when ``secret`` is set). With an
    inbox, 202 means the dispatch envelope is committed, not that the far side is reachable, and
    redeliveries reuse the receipt. The synchronous path remains for embedded callers: in managed
    mode it forwards the backend's own 409/422/429/503 so a drain or a capacity refusal is not
    mistaken for an outage, and a 502 stays reserved for a boundary that could not be reached.
    ``GET /healthz`` -> 200. Log lines carry the delivery id and the outcome, never the body or a
    secret.
    """
    emit = log if log is not None else lambda line: print(line, file=sys.stderr, flush=True)

    class Handler(BaseHTTPRequestHandler):
        server_version = "swfactory-webhook/2"

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(10)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
            return None  # one structured line per event instead (see _reply)

        def do_GET(self) -> None:  # noqa: N802 - stdlib naming
            if self.path.split("?", 1)[0] == HEALTH_PATH:
                self._reply(200, {"ok": True})
            elif self.path.split("?", 1)[0] == READY_PATH and inbox is not None:
                try:
                    summary = inbox.summary()
                except (sqlite3.Error, OSError):
                    self._reply(503, {"ok": False, "error": "inbox unavailable"})
                    return
                alive = dispatcher is not None and dispatcher.thread.is_alive()
                capacity = (
                    sum(summary["counts"][state] for state in ("pending", "dispatching", "dead")) < inbox.max_pending
                )
                ready = alive and capacity
                self._reply(200 if ready else 503, {"ok": ready, **summary})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802 - stdlib naming
            if self.path.split("?", 1)[0] != WEBHOOK_PATH:
                self._reply(404, {"error": "not found"})
                return
            if self.headers.get("Transfer-Encoding"):
                self._reply(400, {"error": "Transfer-Encoding is unsupported"})
                return
            if len(self.headers.get_all("Content-Length", [])) != 1:
                self._reply(400, {"error": "one Content-Length header is required"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                self._reply(400, {"error": "invalid Content-Length"})
                return
            if length < 0 or length > MAX_BODY_BYTES:
                self._reply(413, {"error": "payload too large"})
                return
            try:
                body = self.rfile.read(length) if length > 0 else b""
            except OSError:
                self._reply(408, {"error": "request body timed out"})
                return
            if len(body) != length:
                self._reply(400, {"error": "incomplete request body"})
                return
            delivery = self.headers.get("X-GitHub-Delivery", "-")
            event = self.headers.get("X-GitHub-Event", "")
            if secret is not None and not verify_signature(secret, body, self.headers.get(SIGNATURE_HEADER)):
                self._reply(401, {"error": "bad signature"}, delivery, event)
                return
            try:
                payload = json.loads(body.decode("utf-8"))
            except ValueError:
                self._reply(400, {"error": "invalid JSON"}, delivery, event)
                return
            trigger = route(event, payload if isinstance(payload, dict) else {})
            if trigger is None:
                self._reply(200, {"routed": False, "event": event}, delivery, event)
                return
            if inbox is not None:
                self._enqueue(trigger, payload, body, delivery, event)
                return
            if work_orders is not None:
                self._submit_now(trigger, delivery, event)
                return
            try:
                assert token_provider is not None  # enforced by make_server
                run_id = trigger_airflow(trigger, airflow_url=airflow_url, token=token_provider(), opener=opener)
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", errors="replace")[:500]
                self._reply(
                    502,
                    {"error": f"airflow HTTP {e.code}", "detail": detail, "dag_id": trigger.dag_id},
                    delivery,
                    event,
                )
                return
            except (urllib.error.URLError, OSError, RuntimeError, ValueError) as e:
                self._reply(502, {"error": f"airflow unreachable: {e}", "dag_id": trigger.dag_id}, delivery)
                return
            self._reply(
                202,
                {
                    "routed": True,
                    "dag_id": trigger.dag_id,
                    "conf": trigger.conf,
                    "dag_run_id": run_id,
                },
                delivery,
                event,
            )

        def _submit_now(self, trigger: Trigger, delivery: str, event: str) -> None:
            """Inboxless managed mode: the backend's own refusal is what the caller is told.

            A drain, a capacity refusal or an immutable-work conflict is forwarded with its status
            instead of being flattened into 502, because each one means something different to the
            sender and none of them is "Airflow is down".
            """
            assert work_orders is not None
            try:
                receipt = submit_work_order(trigger, work_orders, opener=opener)
            except urllib.error.HTTPError as e:
                e.close()
                status = e.code if e.code in {409, 422, 429, 503} else 502
                detail = {"error": f"work order refused ({e.code})", "dag_id": trigger.dag_id}
                self._reply(status, detail, delivery, event)
                return
            except (urllib.error.URLError, OSError, RuntimeError, ValueError) as e:
                self._reply(502, {"error": f"backend unreachable: {e}", "dag_id": trigger.dag_id}, delivery, event)
                return
            self._reply(
                202,
                {
                    "routed": True,
                    "dag_id": trigger.dag_id,
                    "conf": trigger.conf,
                    "submission_id": receipt["submission_id"],
                    "state": receipt.get("state"),
                    "dag_run_id": receipt.get("run_id"),
                },
                delivery,
                event,
            )

        def _enqueue(
            self,
            trigger: Trigger,
            payload: Mapping[str, Any],
            body: bytes,
            delivery_id: str,
            event: str,
        ) -> None:
            assert inbox is not None
            try:
                repository = _repository(payload)
                # A receipt survives blueprint edits/removal. New deliveries must still pass
                # current routing policy; duplicate bodies must match the saved digest.
                try:
                    receipt = inbox.get(delivery_id)
                except KeyError:
                    trigger = repository_trigger(trigger, repository)
                else:
                    trigger = Trigger(receipt.dag_id, receipt.conf, receipt.dag_run_id)
                receipt, created = inbox.enqueue(delivery_id, event, body, repository, trigger)
            except DeliveryConflict as exc:
                self._reply(409, {"error": str(exc)}, delivery_id, event)
                return
            except FileNotFoundError:
                self._reply(422, {"error": "blueprint is not installed"}, delivery_id, event)
                return
            except ValueError:
                self._reply(422, {"error": "invalid delivery or repository route"}, delivery_id, event)
                return
            except (InboxFull, sqlite3.Error, OSError):
                self._reply(503, {"error": "inbox unavailable or full"}, delivery_id, event)
                return
            if dispatcher is not None:
                dispatcher.wakeup.set()
            self._reply(
                202,
                {
                    "routed": True,
                    "delivery_id": receipt.delivery_id,
                    "dag_id": receipt.dag_id,
                    "dag_run_id": receipt.dag_run_id,
                    "state": receipt.state,
                    "duplicate": not created,
                },
                delivery_id,
                event,
            )

        def _reply(self, status: int, doc: Mapping[str, Any], delivery: str = "-", event: str = "") -> None:
            data = json.dumps(doc).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            # An acknowledged-in-storage delivery survives a disconnected sender: the receipt is
            # already durable, so failing to write the response body changes nothing that matters.
            with contextlib.suppress(OSError):
                self.wfile.write(data)
            summary = doc.get("dag_id") or doc.get("error") or doc.get("event") or ""
            safe_delivery = re.sub(r"[^A-Za-z0-9_-]", "?", delivery)[:128]
            emit(f"webhook {self.command} -> {status} delivery={safe_delivery} {summary!r}")

    return Handler


def make_server(
    port: int,
    *,
    airflow_url: str,
    token_provider: TokenProvider | None = None,
    secret: str | None = None,
    opener: Opener = _NO_REDIRECT_OPENER,
    host: str = "0.0.0.0",
    log: Callable[[str], None] | None = None,
    inbox: DeliveryInbox | None = None,
    max_attempts: int = 12,
    work_orders: WorkOrders | None = None,
) -> HTTPServer:
    """A bound (not yet serving) ``ThreadingHTTPServer``; ``port=0`` picks an ephemeral port
    (``server.server_address[1]``) — what the tests use."""
    if work_orders is None and token_provider is None:
        raise ValueError("legacy direct-Airflow mode needs an Airflow token provider")
    dispatcher = None
    if inbox is not None:
        dispatcher = Dispatcher(
            inbox,
            airflow_url=airflow_url,
            token_provider=token_provider,
            opener=opener,
            log=log or (lambda line: print(line, file=sys.stderr, flush=True)),
            max_attempts=max_attempts,
            work_orders=work_orders,
        )
    handler = make_handler(
        airflow_url=airflow_url,
        token_provider=token_provider,
        secret=secret,
        opener=opener,
        log=log,
        inbox=inbox,
        dispatcher=dispatcher,
        work_orders=work_orders,
    )

    class Server(ThreadingHTTPServer):
        def server_close(self) -> None:
            if dispatcher is not None and dispatcher.thread.ident is not None:
                dispatcher.close()
            super().server_close()

    server = Server((host, port), handler)
    if dispatcher is not None:
        dispatcher.start()
    return server


def serve(
    port: int,
    *,
    airflow_url: str,
    token_provider: TokenProvider | None = None,
    secret: str | None = None,
    opener: Opener = _NO_REDIRECT_OPENER,
    host: str = "0.0.0.0",
    inbox: DeliveryInbox | None = None,
    max_attempts: int = 12,
    work_orders: WorkOrders | None = None,
) -> None:
    """Run the receiver until interrupted (``swfactory webhook serve``)."""
    server = make_server(
        port,
        airflow_url=airflow_url,
        token_provider=token_provider,
        secret=secret,
        opener=opener,
        host=host,
        inbox=inbox,
        max_attempts=max_attempts,
        work_orders=work_orders,
    )
    bound = server.server_address[1]
    # The banner names the boundary this receiver actually submits through, so an operator can see
    # from one line whether runs will be managed or legacy unmanaged Airflow writes.
    destination = f"{work_orders.url}{WORK_ORDERS_PATH}" if work_orders else f"{airflow_url} (LEGACY unmanaged)"
    print(
        f"webhook: listening on {host}:{bound} (POST {WEBHOOK_PATH}, GET {HEALTH_PATH}) -> "
        f"{destination} signature={'local' if secret else 'upstream (islo)'}",
        file=sys.stderr,
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
