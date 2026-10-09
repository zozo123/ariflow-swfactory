"""What credentialed HTTP clients share: the backend token rule, one bounded JSON ``POST``, no redirects.

Stdlib only. The worker-side clients of the factory backend (managed SCM, Cell callbacks, population
execution, the autonomy status view) all post through ``post_json``; a client whose credential must
not follow a redirect to another origin opens through ``no_redirect_open``.
"""

from __future__ import annotations

import ipaddress
import json
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

# The backend rejects anything shorter (``backend/service.py``), so a shorter value is not a
# weak configuration: it is one that cannot authenticate at all.
MIN_BACKEND_TOKEN_CHARS = 32
_ERROR_BODY_BYTES = 8192


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow a redirect: one would carry the Authorization header to another origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ResponseTooLarge(ValueError):
    """A response body over the caller's limit; never decoded."""


def valid_backend_token(token: str) -> bool:
    return len(token) >= MIN_BACKEND_TOKEN_CHARS and not any(c.isspace() for c in token)


def is_loopback_host(host: str) -> bool:
    """``localhost`` or a loopback IP literal; any other name could resolve anywhere."""
    try:
        return host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def no_redirect_open(request: urllib.request.Request, *, timeout: float) -> Any:
    return urllib.request.build_opener(NoRedirect()).open(request, timeout=timeout)


def post_json(
    base: str,
    token: str,
    path: str,
    body: dict[str, Any],
    *,
    timeout: float,
    limit: int,
    opener: Callable[..., Any] | None = None,
) -> tuple[int, Any]:
    """One bearer-authenticated JSON ``POST``; ``(status, decoded body)``, an empty body as ``{}``.

    An HTTP error status is returned, not raised, with the backend's ``detail`` (else
    ``HTTP <status>``) in place of the body. Transport failure raises ``OSError``, a body over
    ``limit`` bytes ``ResponseTooLarge`` and a success body that is not JSON ``ValueError``.
    ``opener`` defaults to ``urllib.request.urlopen``, looked up per call.
    """
    request = urllib.request.Request(
        base + path,
        data=json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
        method="POST",
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with (opener or urllib.request.urlopen)(request, timeout=timeout) as response:
            raw = response.read(limit + 1)
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read(_ERROR_BODY_BYTES)
        status = error.code
    if len(raw) > limit:
        raise ResponseTooLarge(f"response exceeds {limit} bytes")
    if status < 300:
        return status, json.loads(raw) if raw else {}
    try:
        value = json.loads(raw) if raw else {}
    except ValueError:
        value = {}
    detail = value.get("detail") if isinstance(value, dict) else None
    return status, str(detail or f"HTTP {status}")
