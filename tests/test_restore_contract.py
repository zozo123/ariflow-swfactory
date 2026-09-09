"""The backup, migration and restore contract for whole-factory state (#2074).

The failure this module exists for is the worst kind a factory has: a restored old snapshot
silently replaying a publication that already happened. The journal believes an operation is
pending; the outside world already has the pull request.

Everything here is hermetic, and a "restore" is a real restore -- every store object is closed, the
bytes on disk are replaced, and new store objects are opened on the same directory. Anything the
tests observe afterwards came out of SQLite and the evidence files rather than out of a live
object. ``FakeGitHub`` counts publications, because a count is the only honest way to say
"exactly once".
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest
from typer.testing import CliRunner

from swfactory.admission import Limits, Priority
from swfactory.cells import CellIdentity, CellStore, StaleEpoch
from swfactory.cleanup_receipt import RepairLeaseStore
from swfactory.cli import app
from swfactory.control_kernel import ControlKernel
from swfactory.deployment_profile import (
    MULTI_HOST_POSTGRES,
    DeploymentProfile,
    UnqualifiedDeployment,
    UnsupportedStateRoot,
    assert_supported_state_root,
    filesystem_type,
)
from swfactory.durable_admission import WORK_ORDER_SCHEMA, DurableAdmission, MemberSpec
from swfactory.idempotency import MutationOutcome, OperationInDoubt, OperationJournal, OperationRef
from swfactory.restore_contract import (
    MANIFEST_NAME,
    PAYLOAD_DIR,
    BackupRefused,
    MutationsWithheld,
    ReconciliationIncomplete,
    RestoreGate,
    RestoreRefused,
    coherence_problems,
    create_backup,
    plan_reconciliation,
    restore,
    status,
    verify_backup,
)
from swfactory.store_schema import StoreSchemaError, StoreSchemaTooNew, assert_compatible
from swfactory.trust_evidence import TrustedEvidence

REPO = "acme/widgets"
ISSUE = "2074"
DOC = Path(__file__).resolve().parents[1] / "docs" / "backup-restore.md"


def _identity(issue: str = ISSUE) -> CellIdentity:
    return CellIdentity(REPO, "main", issue)


def _order(issue: str = ISSUE) -> dict:
    return {"schema_version": WORK_ORDER_SCHEMA, "repo": REPO, "issue": issue, "blueprint": "default"}


class FakeGitHub:
    """A remote that counts publications and can be asked what it already has.

    ``calls`` is an ordered log rather than a pair of counters, because "did it reconcile before it
    acted" is a question about order that a count cannot answer.
    """

    def __init__(self) -> None:
        self.published: list[str] = []
        self.effects: list[tuple[str, str]] = []
        self.calls: list[str] = []

    def publish(self, cell_id: str, tag: str = "pull-request") -> dict:
        self.published.append(cell_id)
        self.effects.append((cell_id, tag))
        self.calls.append(f"publish:{cell_id}:{tag}")
        return {"pull_request": f"https://github.invalid/{REPO}/pull/{len(self.published)}"}

    def observe(self, cell_id: str, tag: str = "pull-request") -> MutationOutcome:
        """What a reconciler learns by asking the remote instead of guessing from the journal."""
        self.calls.append(f"observe:{cell_id}:{tag}")
        for index, (published, published_tag) in enumerate(self.effects, start=1):
            if published == cell_id and published_tag == tag:
                return MutationOutcome(
                    "committed",
                    {"pull_request": f"https://github.invalid/{REPO}/pull/{index}"},
                    {"source": "remote"},
                )
        return MutationOutcome("definitely_absent", None, {"source": "remote"})


class FactoryState:
    """The four authoritative stores plus evidence, opened on one shared local state root."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.gate = RestoreGate(self.root)
        self.cells = CellStore(self.root / "cells.sqlite3")
        self.kernel = ControlKernel(
            self.root / "control",
            limits=Limits(global_active=1),
            restore_gate=self.gate,
        )
        self.evidence = TrustedEvidence(self.root / "evidence")

    @property
    def journal(self) -> OperationJournal:
        return self.kernel.operations

    @property
    def admission(self) -> DurableAdmission:
        return self.kernel.admission

    @property
    def repairs(self) -> RepairLeaseStore:
        return self.kernel.repairs

    def close(self) -> None:
        self.cells.close()
        self.kernel.close()


@pytest.fixture
def github() -> FakeGitHub:
    return FakeGitHub()


def _dies_after_send() -> dict:
    raise ConnectionError("socket closed after the request was sent")


def _seed(state: FactoryState, github: FakeGitHub) -> dict[str, object]:
    """A factory caught mid-flight: queued work, a committed publication, an in-doubt effect, debt.

    Exactly the snapshot content acceptance box 1 names, built through the real stores so a restore
    is judged against records the running factory would actually have written.
    """
    live = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(live["cell_id"]), int(live["epoch"])
    state.admission.submit(
        work_id="submit_live",
        actor="operator",
        blueprint="default",
        order=_order(),
        members=[MemberSpec(0, REPO, cell_id)],
        priority=Priority.NORMAL,
    )

    # Queued work: capacity is one Cell, so this second order cannot dispatch. It must survive a
    # restore as a reservation, not as work the restored factory forgets it owes anyone.
    queued_cell = state.cells.activate(_identity("2075"), "operator")
    queued = state.admission.submit(
        work_id="submit_queued",
        actor="operator",
        blueprint="default",
        order=_order("2075"),
        members=[MemberSpec(0, REPO, str(queued_cell["cell_id"]))],
        priority=Priority.NORMAL,
    )
    assert queued.state == "queued", queued

    # A committed publication: the remote has the pull request, the journal holds its receipt.
    publish_ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
    receipt = state.kernel.mutate(publish_ref, lambda: github.publish(cell_id))
    state.evidence.append(
        cell_id=cell_id,
        epoch=epoch,
        kind="external_mutation",
        payload={"operation_key": publish_ref.key, "result": receipt},
        policy_digest=None,
    )

    # An in-doubt effect: the request left the process and never answered. Its external outcome is
    # unknown, which is the one state a restore must not resolve by guessing.
    doubt_ref = OperationRef.build(cell_id, epoch, "github_issue", "comment")
    with pytest.raises(ConnectionError):
        state.kernel.mutate(doubt_ref, _dies_after_send)

    # Cleanup debt: a sandbox nobody has proven absent yet.
    state.repairs.acquire(f"cleanup:{cell_id}", "reconciler-1", ttl_s=600.0, metadata={"cell_id": cell_id})
    state.evidence.append(
        cell_id=cell_id,
        epoch=epoch,
        kind="cleanup_debt",
        payload={"sandbox": "sbx-1"},
        policy_digest=None,
    )
    return {
        "cell_id": cell_id,
        "epoch": epoch,
        "queued_cell_id": str(queued_cell["cell_id"]),
        "publish_ref": publish_ref,
        "doubt_ref": doubt_ref,
        "receipt": receipt,
    }


def _restore_and_resume(backup_dir: Path, root: Path, *, reason: str = "drill") -> RestoreGate:
    restore(backup_dir, root, actor="operator", reason=reason, replace_existing=True)
    gate = RestoreGate(root)
    gate.resume(actor="operator", reason=reason)
    return gate


# ------------------------------------------------------- acceptance box 1: a coherent restore


def test_restored_snapshot_keeps_every_identity_and_receipt(tmp_path: Path, github: FakeGitHub) -> None:
    """Queued work, a committed publication, an in-doubt effect and cleanup debt all come back."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    cells_before = {row["cell_id"]: (row["epoch"], row["state"]) for row in state.cells.list()}
    queue_before = [item["work_id"] for item in state.admission.snapshot()["queued"]]
    lease_before = state.repairs.db.execute("SELECT * FROM repair_leases").fetchall()
    evidence_before = state.evidence.verify(str(seeded["cell_id"]))

    manifest = create_backup(root, tmp_path / "backup", actor="operator")
    state.close()
    _restore_and_resume(tmp_path / "backup", root)

    after = FactoryState(root)
    try:
        assert {row["cell_id"]: (row["epoch"], row["state"]) for row in after.cells.list()} == cells_before
        assert [item["work_id"] for item in after.admission.snapshot()["queued"]] == queue_before
        # The committed receipt is replayed byte for byte: identity, not a recomputation.
        publish = after.journal.get(str(seeded["publish_ref"].key))  # type: ignore[union-attr]
        assert publish["state"] == "committed"
        assert publish["result"] == seeded["receipt"]
        # The in-doubt effect stays in doubt. A restore that "tidied" it into committed or absent
        # would be inventing the one fact nobody has.
        doubt = after.journal.get(str(seeded["doubt_ref"].key))  # type: ignore[union-attr]
        assert doubt["state"] == "in_doubt"
        assert [dict(row) for row in after.repairs.db.execute("SELECT * FROM repair_leases")] == [
            dict(row) for row in lease_before
        ]
        assert after.evidence.verify(str(seeded["cell_id"])) == evidence_before
        # And the manifest agrees with the evidence that came back.
        tails = {cell["cell_id"]: cell["tail_digest"] for cell in manifest["evidence"]["cells"]}
        assert tails[str(seeded["cell_id"])] == evidence_before[1]
    finally:
        after.close()


def test_a_backup_covers_every_authoritative_store(tmp_path: Path, github: FakeGitHub) -> None:
    """All five stores, or it is not a backup of this factory."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    manifest = create_backup(root, tmp_path / "backup", actor="operator")
    state.close()
    assert {store["name"] for store in manifest["stores"]} == {
        "cells",
        "operations",
        "admission",
        "repairs",
        "evidence",
    }
    assert all(store["present"] for store in manifest["stores"])
    assert manifest["quiesced"] is True


# ------------------------------------- acceptance box 2: refusals with an actionable diagnostic


def test_copying_only_the_database_files_is_refused_as_a_backup(tmp_path: Path, github: FakeGitHub) -> None:
    """`cp state/*.sqlite3 backup/` catches WAL-resident commits mid-write and looks restorable."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)

    hand_made = tmp_path / "partial"
    hand_made.mkdir()
    for path in sorted(root.rglob("*.sqlite3")):
        target = hand_made / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    state.close()

    # The hand-made copy really did lose the committed publication, which is why it must never be
    # treated as restorable: a factory restored from it would publish that pull request again.
    journal = OperationJournal(hand_made / "control" / "operations.sqlite3")
    try:
        with pytest.raises(KeyError):
            journal.get(str(seeded["publish_ref"].key))  # type: ignore[union-attr]
    finally:
        journal.close()

    verification = verify_backup(hand_made)
    assert not verification.ok
    assert any(MANIFEST_NAME in problem for problem in verification.problems)
    with pytest.raises(RestoreRefused, match="not restorable"):
        restore(hand_made, tmp_path / "elsewhere", actor="operator", reason="drill")


def test_a_backup_missing_one_file_is_refused(tmp_path: Path, github: FakeGitHub) -> None:
    """Three of four stores is worse than none, because it looks restorable."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()
    (backup / PAYLOAD_DIR / "control" / "repairs.sqlite3").unlink()

    verification = verify_backup(backup)
    assert not verification.ok
    assert any("repairs.sqlite3" in problem for problem in verification.problems)
    with pytest.raises(RestoreRefused):
        restore(backup, tmp_path / "elsewhere", actor="operator", reason="drill")


def test_an_edited_backup_is_refused(tmp_path: Path, github: FakeGitHub) -> None:
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()
    db = sqlite3.connect(backup / PAYLOAD_DIR / "cells.sqlite3")
    db.execute("UPDATE cells SET state='cleaned'")
    db.commit()
    db.close()

    verification = verify_backup(backup)
    assert not verification.ok
    assert any("digest" in problem for problem in verification.problems)


def test_a_store_written_by_a_newer_binary_is_refused(tmp_path: Path) -> None:
    """The old-binary rollback refusal, enforced where the store is opened."""
    root = tmp_path / "factory"
    FactoryState(root).close()
    db = sqlite3.connect(root / "cells.sqlite3")
    db.execute("PRAGMA user_version=99")
    db.close()

    with pytest.raises(StoreSchemaTooNew, match="99"):
        CellStore(root / "cells.sqlite3").close()
    with pytest.raises(StoreSchemaError, match="cells"):
        assert_compatible(root)
    assert status(root)["schema_compatible"] is False


def test_restoring_a_newer_backup_with_an_older_binary_is_refused(tmp_path: Path, github: FakeGitHub) -> None:
    """The same refusal at the restore boundary, before a single byte is placed."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    manifest = create_backup(root, backup, actor="operator")
    state.close()

    # A backup taken by a future binary: rewrite the manifest the way that binary would have, and
    # re-sign it, so the only thing wrong is the schema this binary can own.
    from swfactory.restore_contract import _digest  # the manifest's own signing helper

    for store in manifest["stores"]:
        if store["name"] == "operations":
            store["schema_version"] = 7
    manifest["binary_store_versions"]["operations"] = 7
    body = {k: v for k, v in manifest.items() if k not in {"manifest_digest", "complete"}}
    (backup / MANIFEST_NAME).write_text(
        json.dumps({**body, "manifest_digest": _digest(body), "complete": True}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    assert verify_backup(backup).ok

    target = tmp_path / "target"
    with pytest.raises(RestoreRefused, match="schema 7"):
        restore(backup, target, actor="operator", reason="rollback")
    assert not (target / "cells.sqlite3").exists(), "refused restore must place nothing"


def test_missing_evidence_refuses_to_resume_mutations(tmp_path: Path, github: FakeGitHub) -> None:
    """A Cell whose history is gone cannot prove what it already did to the outside world."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    shutil.rmtree(root / "evidence" / str(seeded["cell_id"]))
    with pytest.raises(RestoreRefused, match="evidence for restored Cell"):
        RestoreGate(root).resume(actor="operator", reason="drill")
    assert status(root)["mutations_allowed"] is False


def test_a_partial_state_root_refuses_readiness(tmp_path: Path, github: FakeGitHub) -> None:
    """A restored factory missing a store is a partial restore, not a fresh one."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    (root / "control" / "admission.sqlite3").unlink()
    with pytest.raises(RestoreRefused, match="admission"):
        RestoreGate(root).resume(actor="operator", reason="drill")


def test_a_truncated_store_refuses_rather_than_recreating_its_tables(tmp_path: Path) -> None:
    """An empty file where a store belongs must not be filled in with fresh empty tables."""
    root = tmp_path / "factory"
    FactoryState(root).close()
    db = sqlite3.connect(root / "cells.sqlite3")
    db.execute("DROP TABLE cell_events")
    db.commit()
    db.close()
    with pytest.raises(StoreSchemaError, match="cell_events"):
        CellStore(root / "cells.sqlite3").close()


# -------------------------------- acceptance box 3: reconciliation precedes replay


def test_restored_snapshot_cannot_replay_a_committed_publication(tmp_path: Path, github: FakeGitHub) -> None:
    """The headline failure: an old snapshot re-driving an effect the world already has."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)

    # The nightly backup, taken before this publication was even attempted.
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")

    ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
    state.kernel.mutate(ref, lambda: github.publish(cell_id))
    assert github.published == [cell_id]
    state.close()

    _restore_and_resume(backup, root)
    restored = FactoryState(root)
    try:
        # No journal row, no observation: the restored factory refuses rather than guessing that a
        # missing row means a missing pull request.
        with pytest.raises(OperationInDoubt):
            restored.kernel.mutate(ref, lambda: github.publish(cell_id))
        assert github.published == [cell_id]

        # With a reconciler it adopts the pull request that already exists instead of opening a
        # second one, and the receipt it returns is the remote's.
        adopted = restored.kernel.mutate(
            ref,
            lambda: github.publish(cell_id),
            reconcile=lambda: github.observe(cell_id),
        )
        assert github.published == [cell_id]
        assert adopted == {"pull_request": f"https://github.invalid/{REPO}/pull/1"}
    finally:
        restored.close()


def test_mutations_are_withheld_until_the_restore_is_validated(tmp_path: Path, github: FakeGitHub) -> None:
    """Restore validation happens before mutations resume, not alongside them."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    restored = FactoryState(root)
    try:
        ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
        with pytest.raises(MutationsWithheld, match="backup resume"):
            restored.kernel.mutate(ref, lambda: github.publish(cell_id))
        assert github.published == []
        assert status(root)["mutations_allowed"] is False

        restored.gate.resume(actor="operator", reason="checked")
        assert status(root)["mutations_allowed"] is True
    finally:
        restored.close()


def test_a_reconciled_cell_leaves_the_worklist_but_not_the_window(tmp_path: Path, github: FakeGitHub) -> None:
    """Observation is the only way out of the worklist, and the journal is what proves it."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    cell_id = str(seeded["cell_id"])
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    gate = _restore_and_resume(backup, root)
    assert gate.requires_observation(cell_id) is True

    # The in-doubt operation has no recorded observation, so the claim is refused outright.
    with pytest.raises(ReconciliationIncomplete, match="no recorded observation"):
        gate.mark_reconciled(cell_id=cell_id)

    restored = FactoryState(root)
    try:
        restored.journal.observe(seeded["doubt_ref"], lambda: github.observe("nothing"))  # type: ignore[arg-type]
    finally:
        restored.close()
    marker = gate.mark_reconciled(cell_id=cell_id)
    assert cell_id not in marker["unreconciled_cells"]
    # Off the worklist, still inside the window: the restore's remaining risk is the effects the
    # snapshot never recorded, and reconciling the rows it did record says nothing about those.
    assert gate.requires_observation(cell_id) is True
    assert gate.state == "reconciling"


def test_an_in_flight_dispatch_intent_is_carried_into_the_gate(tmp_path: Path, github: FakeGitHub) -> None:
    """#2058's outbox already models an in-doubt delivery; the restore gate reuses it."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    state.admission.db.execute("UPDATE admission_dispatch SET state='inflight' WHERE work_id='submit_live'")
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    marker = restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    assert "submit_live" in marker["unreconciled_dispatch"]
    gate = RestoreGate(root)
    gate.resume(actor="operator", reason="checked")
    with pytest.raises(ReconciliationIncomplete, match="still in flight"):
        gate.mark_reconciled(work_id="submit_live")

    plan = plan_reconciliation(root)
    assert plan["dispatch"] == ["submit_live"]
    # The in-doubt operation is carried with the action #2042's recovery planner would take. It may
    # be waiting out its backoff, but it is never "retry": a restored in-doubt effect that recovery
    # proposes retrying is the duplicate publication this whole contract exists to prevent.
    doubt_key = str(seeded["doubt_ref"].key)  # type: ignore[union-attr]
    rows = {row["operation_key"]: row for row in plan["operations"]}
    assert rows[doubt_key]["state"] == "in_doubt"
    assert rows[doubt_key]["action"] in {"observe", "wait"}
    assert all(row["action"] != "retry" for row in plan["operations"])


def test_the_window_closes_only_on_an_operator_statement(tmp_path: Path, github: FakeGitHub) -> None:
    """Emptying the worklist is not the same fact as reviewing the interval the snapshot missed."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    gate = _restore_and_resume(backup, root)
    with pytest.raises(ReconciliationIncomplete, match="still unreconciled"):
        gate.close(actor="operator", reason="drill", window_reviewed=True)
    gate.mark_reconciled(cell_id=cell_id)
    # Every restored row is reconciled and the window is still open, because the effects that can
    # be duplicated are the ones no restored row mentions.
    with pytest.raises(ReconciliationIncomplete, match="restore window covers"):
        gate.close(actor="operator", reason="drill")
    assert status(root)["observation_required"] is True

    marker = gate.close(actor="operator", reason="drill", window_reviewed=True)
    assert marker["state"] == "clear"
    assert gate.requires_observation(cell_id) is False
    assert not gate.path.exists()
    # The decision is retained, not forgotten: the archive is the drill's evidence.
    archives = list((root / "restore").glob("completed-*.json"))
    assert len(archives) == 1
    history = json.loads(archives[0].read_text())["history"]
    assert history[-1]["event"] == "closed"
    assert history[-1]["actor"] == "operator"
    assert status(root)["mutations_allowed"] is True
    assert status(root)["observation_required"] is False


# ------------------------- acceptance box 4: the drill, operator commands and the boundary


def test_the_operator_drill_runs_end_to_end_through_the_cli(tmp_path: Path, github: FakeGitHub) -> None:
    """Exactly the documented sequence: create, verify, restore, status, resume, reconciled."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    cell_id = str(seeded["cell_id"])
    backup = tmp_path / "backup"
    runner = CliRunner()

    assert runner.invoke(app, ["backup", "create", str(backup), "--state-root", str(root)]).exit_code == 0
    state.close()
    assert runner.invoke(app, ["backup", "verify", str(backup)]).exit_code == 0

    restored = runner.invoke(
        app,
        ["backup", "restore", str(backup), "--state-root", str(root), "--reason", "drill", "--replace-existing"],
    )
    assert restored.exit_code == 0, restored.output
    assert "WITHHELD" in restored.output

    # Mutations withheld: exit 1 is the contract, so a drill script cannot miss it.
    withheld = runner.invoke(app, ["backup", "status", "--state-root", str(root)])
    assert withheld.exit_code == 1
    assert "pending" in withheld.output

    assert runner.invoke(app, ["backup", "resume", "--state-root", str(root), "--reason", "checked"]).exit_code == 0
    refused = runner.invoke(app, ["backup", "reconciled", "--state-root", str(root), "--cell-id", cell_id])
    assert refused.exit_code == 1, refused.output

    after = FactoryState(root)
    try:
        after.journal.observe(seeded["doubt_ref"], lambda: github.observe("nothing"))  # type: ignore[arg-type]
    finally:
        after.close()
    assert runner.invoke(app, ["backup", "reconciled", "--state-root", str(root), "--cell-id", cell_id]).exit_code == 0
    healthy = runner.invoke(app, ["backup", "status", "--state-root", str(root), "--json"])
    assert healthy.exit_code == 0
    assert json.loads(healthy.output)["mutations_allowed"] is True
    # Reconciled is not closed: until an operator says the lost interval was reviewed, every Cell
    # still observes the remote before it acts.
    assert json.loads(healthy.output)["observation_required"] is True
    outstanding = runner.invoke(app, ["backup", "close", "--state-root", str(root), "--reason", "drill"])
    assert outstanding.exit_code == 1
    assert "still unreconciled" in outstanding.output

    settling = FactoryState(root)
    try:
        settling.admission.note_outcome("submit_live", error="checked Airflow", observation={"runs": 1})
    finally:
        settling.close()
    for remaining in RestoreGate(root).outstanding()["cells"]:
        assert (
            runner.invoke(app, ["backup", "reconciled", "--state-root", str(root), "--cell-id", remaining]).exit_code
            == 0
        )
    assert (
        runner.invoke(app, ["backup", "reconciled", "--state-root", str(root), "--work-id", "submit_live"]).exit_code
        == 0
    )
    unreviewed = runner.invoke(app, ["backup", "close", "--state-root", str(root), "--reason", "drill"])
    assert unreviewed.exit_code == 1
    assert "restore window covers" in unreviewed.output
    closed = runner.invoke(
        app,
        ["backup", "close", "--state-root", str(root), "--reason", "drill", "--window-reviewed"],
    )
    assert closed.exit_code == 0, closed.output
    done = runner.invoke(app, ["backup", "status", "--state-root", str(root), "--json"])
    assert done.exit_code == 0
    assert json.loads(done.output)["observation_required"] is False


def test_the_drill_document_only_names_commands_that_exist(tmp_path: Path) -> None:
    """A retained drill that has drifted from the CLI is a drill nobody can run."""
    text = DOC.read_text(encoding="utf-8")
    prefix = "uv run swfactory backup "
    named = sorted(
        {line.strip()[len(prefix) :].split()[0].strip("`.,") for line in text.splitlines() if prefix in line}
    )
    assert named, "the drill must show the operator commands"
    runner = CliRunner()
    for command in named:
        result = runner.invoke(app, ["backup", command, "--help"])
        assert result.exit_code == 0, f"docs name `swfactory backup {command}`, which the CLI does not have"


def test_only_the_single_host_local_state_profile_is_qualified() -> None:
    supported = DeploymentProfile(
        backend_image="ghcr.io/acme/swfactory:2.2.0",
        replicas=1,
        airflow_url="https://airflow.invalid",
        sandbox_capacity=4,
    )
    supported.validate()
    assert supported.to_values()["topology"] == "single-host-local-state"

    with pytest.raises(UnqualifiedDeployment, match="replicas must be 1"):
        DeploymentProfile(
            backend_image="ghcr.io/acme/swfactory:2.2.0",
            replicas=3,
            airflow_url="https://airflow.invalid",
            sandbox_capacity=4,
        ).validate()
    with pytest.raises(UnqualifiedDeployment, match="unqualified"):
        DeploymentProfile(
            backend_image="ghcr.io/acme/swfactory:2.2.0",
            replicas=1,
            airflow_url="https://airflow.invalid",
            sandbox_capacity=4,
            topology=MULTI_HOST_POSTGRES,
        ).validate()
    with pytest.raises(UnqualifiedDeployment, match="Postgres"):
        DeploymentProfile(
            backend_image="ghcr.io/acme/swfactory:2.2.0",
            replicas=1,
            airflow_url="https://airflow.invalid",
            sandbox_capacity=4,
            postgres_dsn_secret="secret://pg",
        ).validate()


def test_a_network_filesystem_state_root_is_refused(tmp_path: Path) -> None:
    """SQLite cannot fence a second writer there, which is the whole boundary."""
    root = tmp_path / "factory"
    root.mkdir()

    def nfs_mounts() -> str:
        return f"server:/export {root} nfs4 rw 0 0\n"

    assert filesystem_type(root, mounts=nfs_mounts) == "nfs4"
    with pytest.raises(UnsupportedStateRoot, match="nfs4"):
        assert_supported_state_root(root, mounts=nfs_mounts)
    # A host that will not say what its filesystem is stays usable: a false refusal here only
    # teaches operators to reach for a bypass flag.
    assert assert_supported_state_root(root, mounts=lambda: "") == "unknown"


def test_a_backup_directory_is_never_written_over(tmp_path: Path) -> None:
    root = tmp_path / "factory"
    FactoryState(root).close()
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    with pytest.raises(BackupRefused, match="not empty"):
        create_backup(root, backup, actor="operator")


def test_restoring_over_live_state_sets_it_aside_rather_than_deleting_it(tmp_path: Path, github: FakeGitHub) -> None:
    """After a bad restore the superseded tree is the only record of what the factory really did."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    with pytest.raises(RestoreRefused, match="replace_existing"):
        restore(backup, root, actor="operator", reason="drill")
    restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    superseded = sorted(tmp_path.glob("factory.superseded-*"))
    assert len(superseded) == 1
    assert (superseded[0] / "cells.sqlite3").is_file()


def test_the_backend_refuses_its_whole_mutation_surface_after_a_restore(tmp_path: Path, github: FakeGitHub) -> None:
    """One refusal at the capability document, so no route can admit work into unvalidated state."""
    from swfactory.backend.service import Factory, Refused

    root = tmp_path / ".factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()
    restore(backup, root, actor="operator", reason="drill", replace_existing=True)

    factory = Factory(token="t" * 40, airflow_url="http://127.0.0.1:8080", state_root=root)
    try:
        capabilities = factory.capabilities()
        assert capabilities["mutation_ready"] is False
        assert "backup resume" in capabilities["detail"]
        with pytest.raises(Refused, match="draining or not mutation-ready") as refused:
            factory.submit({"line": "factory", "issues": ["42"]})
        assert refused.value.status == 503

        factory.restore_gate.resume(actor="operator", reason="checked")
        assert factory.capabilities()["mutation_ready"] is True
    finally:
        factory.close()


def test_the_backend_refuses_to_open_a_state_root_it_must_not_own(tmp_path: Path) -> None:
    from swfactory.backend.service import Factory

    root = tmp_path / ".factory"
    FactoryState(root).close()
    db = sqlite3.connect(root / "control" / "operations.sqlite3")
    db.execute("PRAGMA user_version=42")
    db.close()
    with pytest.raises(StoreSchemaError, match="operations"):
        Factory(token="t" * 40, airflow_url="http://127.0.0.1:8080", state_root=root)


# ------------------- acceptance box 3, continued: the effects the snapshot never recorded


def _publish(state: FactoryState, github: FakeGitHub, cell_id: str, epoch: int, tag: str) -> dict:
    """One managed publication through the real kernel seam, with its evidence."""
    ref = OperationRef.build(cell_id, epoch, "github_publish", tag)
    receipt = state.kernel.mutate(
        ref,
        lambda: github.publish(cell_id, tag),
        reconcile=lambda: github.observe(cell_id, tag),
    )
    state.evidence.append(
        cell_id=cell_id,
        epoch=epoch,
        kind="external_mutation",
        payload={"operation_key": ref.key, "result": receipt},
        policy_digest=None,
    )
    return receipt


def test_a_cell_first_seen_after_the_snapshot_cannot_republish(tmp_path: Path, github: FakeGitHub) -> None:
    """The duplicate this contract exists to prevent, from the Cell the snapshot never had.

    Cell ids are a hash of repo/target/issue, so the factory rebuilds the post-snapshot Cell under
    the same id, finds no journal row for its publication and would open a second pull request. The
    restored rows cannot name it -- which is why the window, not a worklist, decides.
    """
    root = tmp_path / "factory"
    state = FactoryState(root)
    old = state.cells.activate(_identity("1000"), "operator")
    _publish(state, github, str(old["cell_id"]), int(old["epoch"]), "pull-request")
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")

    # The hour the snapshot does not see: a new issue is admitted and its pull request is opened.
    new = state.cells.activate(_identity("2000"), "operator")
    _publish(state, github, str(new["cell_id"]), int(new["epoch"]), "pull-request")
    assert github.published == [str(old["cell_id"]), str(new["cell_id"])]
    state.close()

    gate = _restore_and_resume(backup, root)
    for cell_id in list(gate.outstanding()["cells"]):
        state_after = FactoryState(root)
        state_after.close()
        gate.mark_reconciled(cell_id=cell_id)
    assert gate.outstanding()["cells"] == []
    assert gate.state == "reconciling"

    restored = FactoryState(root)
    try:
        again = restored.cells.activate(_identity("2000"), "operator")
        assert str(again["cell_id"]) == str(new["cell_id"]), "Cell identity is deterministic"
        assert gate.requires_observation(str(again["cell_id"])) is True
        github.calls.clear()
        adopted = _publish(restored, github, str(again["cell_id"]), int(again["epoch"]), "pull-request")
    finally:
        restored.close()
    assert github.calls[0].startswith("observe:"), github.calls
    assert github.published == [str(old["cell_id"]), str(new["cell_id"])]
    assert adopted == {"pull_request": f"https://github.invalid/{REPO}/pull/2"}


def test_a_reconciled_cell_does_not_replay_an_effect_the_snapshot_never_saw(tmp_path: Path, github: FakeGitHub) -> None:
    """Reconciling the rows that came back proves nothing about the rows that did not."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    _publish(state, github, cell_id, epoch, "pull-request")
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    _publish(state, github, cell_id, epoch, "release-note")  # after the snapshot, same Cell
    state.close()

    gate = _restore_and_resume(backup, root)
    # Every restored operation is committed, so the claim is accepted -- and it covers only them.
    gate.mark_reconciled(cell_id=cell_id)
    restored = FactoryState(root)
    try:
        github.calls.clear()
        _publish(restored, github, cell_id, epoch, "release-note")
    finally:
        restored.close()
    assert github.calls[0].startswith("observe:"), github.calls
    assert github.effects == [(cell_id, "pull-request"), (cell_id, "release-note")]


def test_a_restore_without_a_reconciler_refuses_rather_than_acting(tmp_path: Path, github: FakeGitHub) -> None:
    """While the window is open, a caller with nothing to observe with does not get to guess."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity("2000"), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    gate = _restore_and_resume(backup, root)
    restored = FactoryState(root)
    try:
        ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
        with pytest.raises(OperationInDoubt):
            restored.kernel.mutate(ref, lambda: github.publish(cell_id))
        assert github.published == []
        # The refusal left a durable intent, so the way out is a recorded observation, not a claim.
        restored.journal.observe(ref, lambda: github.observe(cell_id))
        gate.mark_reconciled(cell_id=cell_id)
        gate.close(actor="operator", reason="drill", window_reviewed=True)
        # Once the operator has ended the window the factory acts on its own journal again, and a
        # first attempt no longer needs a reconciler to be allowed to happen at all.
        assert gate.requires_observation(cell_id) is False
        other = restored.cells.activate(_identity("3000"), "operator")
        other_ref = OperationRef.build(str(other["cell_id"]), int(other["epoch"]), "github_publish", "pull-request")
        restored.kernel.mutate(other_ref, lambda: github.publish(str(other["cell_id"])))
        assert github.published == [str(other["cell_id"])]
    finally:
        restored.close()


# ------------------------------ acceptance box 1, continued: state a restore must not delete


def test_run_directories_survive_a_backup_and_restore(tmp_path: Path, github: FakeGitHub) -> None:
    """The accepted-inputs pin and the recorded approvals live in run directories, not the stores.

    A restore replaces the whole state root, so a backup that captured only the five stores would
    verify clean and still delete the record of which inputs a human approved.
    """
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    run = root / "run-a" / "state"
    run.mkdir(parents=True)
    (run / "accepted_inputs.json").write_text(json.dumps({"digest": "sha256:pinned"}), encoding="utf-8")
    (run / "approvals.json").write_text(json.dumps({"deliver": {"digest": "sha256:pinned"}}), encoding="utf-8")

    backup = tmp_path / "backup"
    manifest = create_backup(root, backup, actor="operator")
    state.close()
    assert [entry["run_id"] for entry in manifest["runs"]] == ["run-a"]
    assert verify_backup(backup).ok

    _restore_and_resume(backup, root)
    assert json.loads((run / "accepted_inputs.json").read_text())["digest"] == "sha256:pinned"
    assert json.loads((run / "approvals.json").read_text())["digest" if False else "deliver"]["digest"] == (
        "sha256:pinned"
    )
    assert str(seeded["cell_id"]) in RestoreGate(root).outstanding()["cells"]


def test_a_tampered_run_directory_is_refused_like_any_other_state(tmp_path: Path, github: FakeGitHub) -> None:
    """Run files are covered by the manifest digests, or they are not covered at all."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    run = root / "run-a" / "state"
    run.mkdir(parents=True)
    (run / "accepted_inputs.json").write_text(json.dumps({"digest": "sha256:pinned"}), encoding="utf-8")
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    edited = backup / PAYLOAD_DIR / "run-a" / "state" / "accepted_inputs.json"
    edited.write_text(json.dumps({"digest": "sha256:someone-elses"}), encoding="utf-8")
    verification = verify_backup(backup)
    assert not verification.ok
    assert any("accepted_inputs.json" in problem for problem in verification.problems)
    with pytest.raises(RestoreRefused):
        restore(backup, tmp_path / "elsewhere", actor="operator", reason="drill")


def test_a_restore_names_every_mutation_fence_it_rolled_back(tmp_path: Path, github: FakeGitHub) -> None:
    """#2041's fence is monotonic in a running factory and is not monotonic across a restore.

    The restored rows become the authority, so a worker the live factory had already fenced out at
    a higher epoch can write again. The contract cannot prevent that -- it can refuse to be quiet
    about it.
    """
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.cells.take_epoch(cell_id, epoch, "repair")
    with pytest.raises(StaleEpoch):
        state.cells.patch(cell_id, epoch, "op:stale", state="dispatching")
    state.close()

    marker = restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    assert marker["fence_rollbacks"] == [{"cell_id": cell_id, "was": epoch + 1, "now": epoch}]
    assert RestoreGate(root).outstanding()["fence_rollbacks"] == marker["fence_rollbacks"]
    report = status(root)
    assert report["restore"]["fence_rollbacks"][0]["was"] == epoch + 1

    restored = FactoryState(root)
    try:
        # The write that was fenced out before the restore is accepted after it. That is the fact
        # the marker exists to state; the operator, not the factory, is the one who can act on it.
        restored.cells.patch(cell_id, epoch, "op:stale", state="dispatching")
    finally:
        restored.close()


def test_a_restored_in_flight_attempt_is_never_planned_as_a_retry(tmp_path: Path, github: FakeGitHub) -> None:
    """A snapshot taken mid-attempt cannot prove the attempt never reached the provider."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
    state.journal.begin(ref)
    state.journal.start_attempt(ref)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    _restore_and_resume(backup, root)
    plan = plan_reconciliation(root)
    rows = {row["operation_key"]: row for row in plan["operations"]}
    assert rows[ref.key]["action"] == "observe"
    assert rows[ref.key]["reason"] == "restored_state_cannot_prove_this_attempt_never_landed"

    restored = FactoryState(root)
    try:
        # And the executing path agrees with the plan: the claim outlived its process, so the
        # operation is in doubt rather than attempted again.
        with pytest.raises(OperationInDoubt):
            restored.kernel.mutate(ref, lambda: github.publish(cell_id), reconcile=lambda: github.observe(cell_id))
        assert github.published == []
    finally:
        restored.close()


# ------------------------------ acceptance box 4: the joins between the stores, not inside them


def test_stores_that_came_from_two_different_instants_are_refused(tmp_path: Path, github: FakeGitHub) -> None:
    """Every file can verify while the set of them is incoherent -- the restore that looks fine."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    state.close()
    assert coherence_problems(root) == []

    # The journal is newer than the Cell store: an operation bound to an epoch the Cells never
    # reached. The two files are individually perfect.
    db = sqlite3.connect(root / "control" / "operations.sqlite3")
    db.execute("UPDATE operations SET epoch=9 WHERE operation_key=?", (str(seeded["publish_ref"].key),))
    db.commit()
    db.close()
    problems = coherence_problems(root)
    assert any("journal is newer than the Cell store" in problem for problem in problems), problems

    gate = RestoreGate(root)
    marker = {"schema_version": 1, "state": "pending", "restored_at": 1.0, "restored_cells": [], "history": []}
    (root / "restore").mkdir(parents=True, exist_ok=True)
    (root / "restore" / "pending.json").write_text(json.dumps(marker), encoding="utf-8")
    with pytest.raises(RestoreRefused, match="do not agree with each other"):
        gate.resume(actor="operator", reason="checked")


def test_a_cell_effect_with_no_receipt_is_refused(tmp_path: Path, github: FakeGitHub) -> None:
    """The dangerous direction: the Cell store remembers an effect whose receipt is missing."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    cell_id, epoch = str(seeded["cell_id"]), int(seeded["epoch"])
    state.close()

    db = sqlite3.connect(root / "cells.sqlite3")
    db.execute(
        "INSERT INTO cell_events(cell_id,epoch,operation_key,kind,payload_json,created_at) VALUES(?,?,?,?,?,?)",
        (cell_id, epoch, "github_publish:deadbeefdeadbeefdeadbeef", "external_mutation", "{}", 0.0),
    )
    db.commit()
    db.close()
    problems = coherence_problems(root)
    assert any("holds no receipt for it" in problem for problem in problems), problems

    # A backup of that state root is refused rather than written and then found wanting.
    with pytest.raises(BackupRefused, match="do not agree with each other"):
        create_backup(root, tmp_path / "backup", actor="operator")


def test_a_hand_assembled_backup_is_checked_for_the_same_joins(tmp_path: Path, github: FakeGitHub) -> None:
    """`verify` re-derives the joins, so a backup someone assembled by hand cannot skip them."""
    from swfactory.restore_contract import _digest, _hash_tree

    root = tmp_path / "factory"
    state = FactoryState(root)
    seeded = _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()
    assert verify_backup(backup).ok

    db = sqlite3.connect(backup / PAYLOAD_DIR / "cells.sqlite3")
    db.execute(
        "INSERT INTO cell_events(cell_id,epoch,operation_key,kind,payload_json,created_at) VALUES(?,?,?,?,?,?)",
        (str(seeded["cell_id"]), int(seeded["epoch"]), "github_publish:deadbeef" * 2, "external_mutation", "{}", 0.0),
    )
    db.commit()
    db.close()
    # Re-manifested with the factory's own helpers: this is a backup whose every file digest is
    # honest and whose stores still disagree, which is the only interesting kind of forgery.
    manifest = json.loads((backup / MANIFEST_NAME).read_text())
    manifest["files"] = _hash_tree(backup, skip={MANIFEST_NAME})
    body = {key: value for key, value in manifest.items() if key not in {"manifest_digest", "complete"}}
    manifest["manifest_digest"] = _digest(body)
    (backup / MANIFEST_NAME).write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    verification = verify_backup(backup)
    assert not verification.ok
    assert any("stores disagree" in problem for problem in verification.problems), verification.problems
    with pytest.raises(RestoreRefused):
        restore(backup, tmp_path / "elsewhere", actor="operator", reason="drill")


def test_verifying_a_backup_does_not_write_into_it(tmp_path: Path, github: FakeGitHub) -> None:
    """A verifier that leaves -wal files behind fails the backup it just proved good."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    _seed(state, github)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()
    before = sorted(path.name for path in (backup / PAYLOAD_DIR).rglob("*"))
    assert verify_backup(backup).ok
    assert verify_backup(backup).ok
    assert sorted(path.name for path in (backup / PAYLOAD_DIR).rglob("*")) == before


def test_deleting_the_restore_marker_does_not_end_the_restore(tmp_path: Path, github: FakeGitHub) -> None:
    """The most natural wrong move after a restore is to delete the thing that is refusing.

    The marker is a file in an operator-writable directory, so the same fact is stamped inside the
    admission store: removing one of the two leaves a factory that refuses and says why, rather than
    one that quietly resumes publishing what it cannot see.
    """
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    restore(backup, root, actor="operator", reason="drill", replace_existing=True)
    shutil.rmtree(root / "restore")
    assert status(root)["mutations_allowed"] is False
    assert status(root)["observation_required"] is True

    restored = FactoryState(root)
    try:
        ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
        with pytest.raises(MutationsWithheld, match="does not end a restore"):
            restored.kernel.mutate(ref, lambda: github.publish(cell_id), reconcile=lambda: github.observe(cell_id))
        assert github.published == []
    finally:
        restored.close()


def test_closing_the_window_clears_both_records_of_it(tmp_path: Path, github: FakeGitHub) -> None:
    """A closed restore leaves nothing behind that would refuse the next start."""
    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)
    backup = tmp_path / "backup"
    create_backup(root, backup, actor="operator")
    state.close()

    gate = _restore_and_resume(backup, root)
    gate.mark_reconciled(cell_id=cell_id)
    gate.close(actor="operator", reason="drill", window_reviewed=True)
    assert RestoreGate(root).state == "clear"
    assert status(root)["mutations_allowed"] is True

    restored = FactoryState(root)
    try:
        ref = OperationRef.build(cell_id, epoch, "github_publish", "pull-request")
        restored.kernel.mutate(ref, lambda: github.publish(cell_id))
        assert github.published == [cell_id]
    finally:
        restored.close()


def test_a_write_landing_while_the_stores_are_being_fenced_refuses_the_backup(
    tmp_path: Path, github: FakeGitHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The locks are taken one at a time, so "one instant" has to be proved, not asserted.

    The hook commits a receipt after the stores' baseline is read and before the last lock is held,
    which is exactly the window a busy factory writes into. Without the check the backup is written,
    says ``quiesced: true``, verifies clean, and holds a journal one moment newer than its Cells.
    """
    from swfactory import restore_contract

    root = tmp_path / "factory"
    state = FactoryState(root)
    cell = state.cells.activate(_identity(), "operator")
    cell_id, epoch = str(cell["cell_id"]), int(cell["epoch"])
    state.evidence.append(cell_id=cell_id, epoch=epoch, kind="activated", payload={}, policy_digest=None)

    real_version = restore_contract._data_version
    reads: list[int] = []

    def version_and_race(db):  # type: ignore[no-untyped-def]
        reads.append(1)
        value = real_version(db)
        if len(reads) == 4:
            # Every store's baseline is read and no lock is held yet: a live factory commits here.
            ref = OperationRef.build(cell_id, epoch, "github_publish", "raced")
            state.journal.execute(ref, lambda: {"pull_request": "raced"})
        return value

    monkeypatch.setattr(restore_contract, "_data_version", version_and_race)
    try:
        with pytest.raises(BackupRefused, match="would not be one instant"):
            create_backup(root, tmp_path / "backup", actor="operator")
    finally:
        monkeypatch.setattr(restore_contract, "_data_version", real_version)
        state.close()
    assert len(reads) >= 4, "the race never happened, so the test proved nothing"
