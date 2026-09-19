from swfactory.jev_triage import Advisory, WorkItem, evaluation_summary, related_issues, shadow_view


def test_vague_upload_high_confidence_never_suppresses_the_proposal() -> None:
    proposal = WorkItem("proposal", "Fix the flaky upload")
    existing = WorkItem("#77", "Make upload reliable")
    advisory = Advisory(
        existing_issue=77,
        relationship="potential_duplicate",
        confidence=0.93,
        probability=0.94,
    )

    view = shadow_view(proposal, related_issues(proposal, [existing]), {"#77": advisory})

    assert view["proposal_preserved"] is True
    assert view["operator_decision_required"] is True
    assert view["authority"] == "none"
    row = view["related"][0]
    assert row["advisory"]["relationship"] == "potential_duplicate"
    assert row["advisory"]["needs_review"] is True
    assert row["acceptance_criteria_missing"] is True


def test_stale_execution_example_keeps_layered_work_visible() -> None:
    proposal = WorkItem(
        "proposal",
        "Reject stale execution requests before a stage handler runs",
        ("stale request returns before handler invocation",),
    )
    fencing = WorkItem(
        "#2228",
        "Port CellStore epoch fencing into Rust",
        ("Rust rejects stale cell epochs",),
    )
    handler = WorkItem(
        "#2255",
        "Require a valid fence before invoking a stage handler",
        ("stale fence keeps handler call count at zero",),
    )
    advisory = Advisory(2255, "potential_duplicate", 0.82, 0.80)

    related = related_issues(proposal, [fencing, handler], limit=2)
    view = shadow_view(proposal, related, {"#2255": advisory})

    assert {row["issue"] for row in view["related"]} == {"#2228", "#2255"}
    handler_row = next(row for row in view["related"] if row["issue"] == "#2255")
    assert handler_row["acceptance_criteria"] == ["stale fence keeps handler call count at zero"]
    assert handler_row["advisory"]["relationship"] == "potential_duplicate"
    assert view["proposal"]["acceptance_criteria"] == ["stale request returns before handler invocation"]


def test_missing_adviser_degrades_to_review_without_losing_related_work() -> None:
    proposal = WorkItem("proposal", "Add retry fence", ("retry uses a fresh epoch",))
    existing = WorkItem("#10", "Fence retries", ("old epoch is rejected",))

    view = shadow_view(proposal, related_issues(proposal, [existing]), {})

    advisory = view["related"][0]["advisory"]
    assert advisory["available"] is False
    assert advisory["needs_review"] is True
    assert view["related"][0]["issue"] == "#10"


def test_evaluation_reports_false_duplicate_suggestions() -> None:
    rows = [
        Advisory(1, "potential_duplicate", 0.93, 0.94, needs_review=True),
        Advisory(2, "extension", 0.8, 0.8),
    ]

    summary = evaluation_summary(["insufficient_evidence", "extension"], rows)

    assert summary == {
        "cases": 2,
        "correct": 1,
        "accuracy": 0.5,
        "needs_review_or_unavailable": 1,
        "false_duplicate_suggestions": 1,
    }
