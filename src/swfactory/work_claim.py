"""Which harness session works which issue, arbitrated by the repository itself.

Many sessions run this factory at once -- each inside its own AI harness, each looping over the
same backlog, each with its own Airflow, backend and state root. `publication_identity` already
makes two of them converge on one branch and one pull request, and refuses an overwrite. But by
the time that fires, both sessions have already run a full agent loop: two sandboxes, two model
budgets, two sets of provider calls. Only the last few seconds were deduplicated. Across N
sessions the wasted fraction approaches (N-1)/N, and that waste is the thing the operator pays
for.

A claim is where the CLI spends the factory's energy: it tells a session which work nobody else is
already burning fuel on.

THE MEDIUM is the repository, for the same reason #2093's coordination service was refused: no new
service, no new credential, no shared datastore. Git's ref creation is already a compare-and-swap
-- pushing a ref that does not exist succeeds for exactly one writer and every other writer is
rejected non-fast-forward. That is a distributed mutex the server enforces, that every instance is
already obliged to obey, and that needs no code anyone has to trust.

A CLAIM AUTHORIZES NOTHING. It is advice about where to spend energy, never permission to publish.
The Cell epoch (#2041) remains the mutation authority; the publication lease
(`scm._apply_and_push`) remains the arbiter of the ref. The publishing path does not read claims
at all, so a stolen, expired or forged claim cannot cause a double publication. That separation is
the lesson of #2093, which failed review precisely because a coordination signal looked like
authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

CLAIM_NAMESPACE = "refs/swf/claims"
DEFAULT_LEASE_S = 3600.0


def claim_ref(key: str) -> str:
    """The ref one publication key's claim lives at."""
    return f"{CLAIM_NAMESPACE}/{key}"


@dataclass(frozen=True)
class Claim:
    """One session's declared intent to work one issue x target, and when it expires."""

    key: str
    instance: str
    at: datetime
    lease_s: float = DEFAULT_LEASE_S

    def message(self) -> str:
        """The claim's commit message: the whole claim, readable by `git log`.

        Kept as text rather than a blob so an operator diagnosing a stuck backlog can read the
        claims with git alone, on a machine that has none of this installed.
        """
        return f"swf-claim {self.key}\n\ninstance={self.instance}\nat={self.at.isoformat()}\nlease_s={self.lease_s:g}\n"

    def expires_at(self) -> datetime:
        return datetime.fromtimestamp(self.at.timestamp() + self.lease_s, tz=UTC)

    def expired(self, *, now: datetime | None = None) -> bool:
        """Whether this claim's lease has run out.

        A harness session dies in ways that leave no trace here -- a context limit, a killed
        container, a spend limit reached mid-loop. A claim with no expiry would strand its issue
        forever; the lease bounds that to one period, and taking it over is itself a
        compare-and-swap, so two sessions recovering the same dead claim cannot both win.
        """
        return (now or datetime.now(UTC)) >= self.expires_at()

    def held_by(self, instance: str) -> bool:
        return self.instance == instance


_FIELD = re.compile(r"^(?P<name>instance|at|lease_s)=(?P<value>.+)$", re.M)


def parse_claim(key: str, message: str) -> Claim | None:
    """Read a claim out of its commit message, or None when the message is not one.

    Strict about the header line: a commit that merely mentions a key in prose is not a claim, and
    treating it as one would let an unrelated ref in this namespace hold the backlog hostage.
    """
    if not message.startswith(f"swf-claim {key}"):
        return None
    fields = {m.group("name"): m.group("value").strip() for m in _FIELD.finditer(message)}
    instance = fields.get("instance", "")
    if not instance:
        return None
    try:
        at = datetime.fromisoformat(fields["at"])
        lease_s = float(fields.get("lease_s", DEFAULT_LEASE_S))
    except (KeyError, ValueError):
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    return Claim(key=key, instance=instance, at=at, lease_s=lease_s)


def may_take(current: Claim | None, *, instance: str, now: datetime | None = None) -> bool:
    """Whether ``instance`` may take this claim.

    Three yeses and one no, and the no is the whole point: another session is alive and working
    this issue, so this session's energy belongs somewhere else.
    """
    if current is None:
        return True  # nobody holds it
    if current.held_by(instance):
        return True  # renewing our own
    return current.expired(now=now)  # the holder is gone; the lease says so


def refusal(current: Claim, *, now: datetime | None = None) -> str:
    """Why this session is not taking the claim, in words an operator can act on."""
    remaining = (current.expires_at() - (now or datetime.now(UTC))).total_seconds()
    return (
        f"{current.key} is claimed by {current.instance}, whose lease has {max(remaining, 0):.0f}s "
        f"left. Another factory session is already working this issue; this one should take other "
        f"work rather than duplicate the spend."
    )
