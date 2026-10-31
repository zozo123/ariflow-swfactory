"""PR conversation context in the existing Cell timeline; never execution authority."""

from __future__ import annotations

import hashlib
from contextlib import suppress

from swfactory.cells import DuplicateOperation, Mutation
from swfactory.control import ControlError, GitHubClient
from swfactory.webhook import TRUSTED_ASSOCIATIONS

from .service import Factory, Refused, text


def receive(factory: Factory, body: dict) -> dict:
    """Re-read a comment and bind it only through a committed host publication receipt."""
    if not factory.capabilities()["mutation_ready"]:
        raise Refused(503, "backend is draining or not mutation-ready")
    repository = text(body, "repository")
    if not factory.repo or repository.casefold() != factory.repo.casefold():
        raise Refused(403, "follow-up belongs to another repository")
    number, comment_id = body.get("pr_number"), body.get("comment_id")
    if type(number) is not int or number <= 0 or type(comment_id) is not int or comment_id <= 0:
        raise ValueError("pr_number and comment_id must be positive integers")
    identifier = "github-comment:" + str(comment_id)
    publications = factory.control.operations.publications_for_pr(factory.repo, number)
    receipt = {"submission_id": identifier, "state": "ignored"}
    if not publications:
        return receipt
    owners = {row["cell_id"] for row in publications}
    if len(owners) != 1:
        raise Refused(409, "PR has ambiguous Cell ownership")
    publication = max(publications, key=lambda row: row["epoch"])
    cell = factory._cell(publication["cell_id"])
    # An already recorded comment is immutable even if GitHub text or Cell epoch moved.
    history = factory.cell_store.history(cell["cell_id"])
    for event in history:
        if event["operation_key"] == identifier:
            return {**receipt, "state": "recorded", "followup": event["payload"]}
    try:
        comment = GitHubClient(factory.repo).issue_comment(comment_id)
    except ControlError as error:
        raise Refused(503, "GitHub comment unavailable; retry intake") from error
    expected_issue = f"https://api.github.com/repos/{factory.repo}/issues/{number}"
    if comment.get("id") != comment_id or comment.get("issue_url", "").casefold() != expected_issue.casefold():
        raise Refused(403, "comment does not belong to the published PR")
    user = comment.get("user") or {}
    if user.get("type") != "User" or not isinstance(user.get("login"), str):
        return receipt
    content = comment.get("body")
    if not isinstance(content, str) or len(content) > 65536:
        raise ValueError("comment body is missing or too long")
    first_line = content.strip().splitlines()[0].strip() if content.strip() else ""
    requested = first_line == "@factory revise"
    trusted = comment.get("author_association") in TRUSTED_ASSOCIATIONS
    disposition = "needs_admission" if requested and trusted else "context"
    if requested and not trusted:
        disposition = "refused"
    stopped = cell["state"] == "cancelled" or any(
        event["epoch"] == publication["epoch"]
        and event["kind"] == "patch"
        and event["payload"].get("state") == "cancelled"
        for event in history
    )
    if stopped and requested:
        disposition = "stopped"
    source_url = f"https://github.com/{factory.repo}/pull/{number}#issuecomment-{comment_id}"
    record = {
        "id": identifier,
        "cell_id": cell["cell_id"],
        "epoch": cell["epoch"],
        "publication_epoch": publication["epoch"],
        "pr_number": number,
        "actor": user["login"],
        "source_url": source_url,
        "body": content,
        "body_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "disposition": disposition,
        "next_action": (
            f"Review {source_url}; incorporate the request into issue {cell['issue']}, "
            "then submit a new managed work order with fresh intent and plan approvals."
            if disposition == "needs_admission"
            else "No execution authorized."
        ),
    }
    try:
        factory.cell_store.record(Mutation(cell["cell_id"], cell["epoch"], identifier, "github_followup", record))
    except DuplicateOperation:
        # A concurrent delivery may have won; return the committed record, never overwrite it.
        return receive(factory, body)
    return {**receipt, "state": "recorded", "followup": record}


def attention(factory: Factory, _body: dict) -> list[dict]:
    """Unresolved explicit revision requests; ordinary discussion creates no attention row."""
    return factory.cell_store.followup_attention()


def dismiss(factory: Factory, body: dict) -> dict:
    """Operator acknowledgement only: no submission, approval, or worker wake."""
    if not factory.capabilities()["mutation_ready"]:
        raise Refused(503, "backend is draining or not mutation-ready")
    cell = factory._cell(text(body, "cell_id"))
    epoch = body.get("epoch")
    if type(epoch) is not int or epoch != cell["epoch"]:
        raise Refused(409, "Cell moved; inspect its current epoch before dismissal")
    identifier = text(body, "id")
    reason = text(body, "reason", max_len=512)
    events = factory.cell_store.history(cell["cell_id"])
    if not any(e["operation_key"] == identifier and e["kind"] == "github_followup" for e in events):
        raise Refused(404, "no such follow-up on this Cell")
    with suppress(DuplicateOperation):
        factory.cell_store.record(
            Mutation(
                cell["cell_id"],
                epoch,
                "dismiss:" + identifier,
                "github_followup_dismissed",
                {"id": identifier, "reason": reason},
            )
        )
    return {"id": identifier, "state": "dismissed"}
