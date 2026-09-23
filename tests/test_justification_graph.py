from __future__ import annotations

import hashlib

import pytest

from swfactory.justification_graph import (
    EdgeKind,
    JustificationEdge,
    JustificationGraph,
    JustificationGraphError,
    JustificationNode,
    NodeKind,
)


def _digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _node(
    quench: str,
    kind: NodeKind,
    statement: str,
    *,
    issuer: str | None = None,
    claim_id: str | None = None,
) -> JustificationNode:
    return JustificationNode(
        quench_digest=quench,
        kind=kind,
        statement=statement,
        payload_digest=_digest(statement),
        claim_id=claim_id,
        issuer=issuer,
    )


def _edge(
    quench: str,
    kind: EdgeKind,
    rule: str,
    premises: tuple[str, ...],
    conclusion: str,
    *,
    verifier: str,
) -> JustificationEdge:
    return JustificationEdge(
        quench_digest=quench,
        kind=kind,
        rule=rule,
        premises=premises,
        conclusion=conclusion,
        verifier=verifier,
        receipt_digest=_digest(f"{rule}:{verifier}:{conclusion}"),
    )


def _graph(*, include_counterexample: bool = False) -> tuple[JustificationGraph, str]:
    quench = _digest("quench")
    artifact = _node(quench, NodeKind.ARTIFACT, "frozen candidate bytes")
    assumption = _node(quench, NodeKind.ASSUMPTION, "GitHub operation ids are stable")
    model = _node(quench, NodeKind.MODEL, "authority transition model")
    evidence = _node(
        quench,
        NodeKind.EVIDENCE,
        "TLC explored the declared authority model",
        issuer="tlc",
    )
    local_claim = _node(
        quench,
        NodeKind.CLAIM,
        "stale epochs cannot commit",
        claim_id="authority.stale-epoch",
    )
    refinement = _node(
        quench,
        NodeKind.REFINEMENT,
        "observed runtime trace conforms to modeled transition vocabulary",
        claim_id="refinement.runtime-authority-model",
    )
    root = _node(
        quench,
        NodeKind.CLAIM,
        "publication preconditions hold for this frozen candidate",
        claim_id="promotion.preconditions",
    )

    edges = [
        _edge(
            quench,
            EdgeKind.SUPPORT,
            "model-check",
            (model.digest(), evidence.digest()),
            local_claim.digest(),
            verifier="model-check-admitter",
        ),
        _edge(
            quench,
            EdgeKind.REFINEMENT,
            "trace-refinement",
            (artifact.digest(), model.digest()),
            refinement.digest(),
            verifier="trace-checker",
        ),
        _edge(
            quench,
            EdgeKind.INFERENCE,
            "promotion-precondition-introduction",
            (local_claim.digest(), refinement.digest(), assumption.digest()),
            root.digest(),
            verifier="claim-kernel",
        ),
    ]
    nodes = [artifact, assumption, model, evidence, local_claim, refinement, root]

    if include_counterexample:
        counterexample = _node(
            quench,
            NodeKind.COUNTEREXAMPLE,
            "replay found a publication outside the modeled precondition",
            issuer="counterexample-checker",
        )
        nodes.append(counterexample)
        edges.append(
            _edge(
                quench,
                EdgeKind.REFUTATION,
                "counterexample-elimination",
                (counterexample.digest(),),
                root.digest(),
                verifier="counterexample-checker",
            )
        )

    return JustificationGraph(quench, tuple(nodes), tuple(edges)), root.digest()


def test_hypergraph_projects_a_claim_only_through_trusted_derivations() -> None:
    graph, root = _graph()
    projection = graph.project(
        root_node=root,
        trusted_verifiers=frozenset(
            {"tlc", "model-check-admitter", "trace-checker", "claim-kernel"}
        ),
    )

    assert projection.root_supported is True
    assert projection.root_refuted is False
    assert projection.justified is True
    assert "promotion.preconditions" in projection.supported_claims
    assert projection.graph_digest == graph.digest()


def test_counterexample_cuts_the_root_even_when_a_support_path_exists() -> None:
    graph, root = _graph(include_counterexample=True)
    projection = graph.project(
        root_node=root,
        trusted_verifiers=frozenset(
            {
                "tlc",
                "model-check-admitter",
                "trace-checker",
                "claim-kernel",
                "counterexample-checker",
            }
        ),
    )

    assert projection.root_supported is False
    assert projection.root_refuted is True
    assert projection.justified is False
    assert "promotion.preconditions" in projection.refuted_claims


def test_untrusted_evidence_cannot_mint_a_derivation() -> None:
    graph, root = _graph()
    projection = graph.project(
        root_node=root,
        trusted_verifiers=frozenset({"model-check-admitter", "trace-checker", "claim-kernel"}),
    )

    assert projection.justified is False
    assert any(
        node.kind == NodeKind.EVIDENCE and node.payload_digest in projection.ignored_objects
        for node in graph.nodes
    )


def test_graph_rejects_cross_quench_nodes() -> None:
    graph, _ = _graph()
    wrong = _node(_digest("other-quench"), NodeKind.ASSUMPTION, "foreign assumption")
    broken = JustificationGraph(graph.quench_digest, graph.nodes + (wrong,), graph.edges)

    with pytest.raises(JustificationGraphError, match="different Formal Quench"):
        broken.validate()


def test_graph_rejects_cycles_in_the_derivation_projection() -> None:
    quench = _digest("cycle-quench")
    a = _node(quench, NodeKind.CLAIM, "claim a", claim_id="a")
    b = _node(quench, NodeKind.CLAIM, "claim b", claim_id="b")
    edges = (
        _edge(
            quench,
            EdgeKind.INFERENCE,
            "a-to-b",
            (a.digest(),),
            b.digest(),
            verifier="checker",
        ),
        _edge(
            quench,
            EdgeKind.INFERENCE,
            "b-to-a",
            (b.digest(),),
            a.digest(),
            verifier="checker",
        ),
    )

    with pytest.raises(JustificationGraphError, match="acyclic"):
        JustificationGraph(quench, (a, b), edges).validate()


def test_graph_identity_changes_when_any_derivation_receipt_changes() -> None:
    graph, _ = _graph()
    first = graph.digest()
    edge = graph.edges[0]
    changed_edge = JustificationEdge(
        quench_digest=edge.quench_digest,
        kind=edge.kind,
        rule=edge.rule,
        premises=edge.premises,
        conclusion=edge.conclusion,
        verifier=edge.verifier,
        receipt_digest=_digest("different-receipt"),
    )
    changed = JustificationGraph(
        graph.quench_digest,
        graph.nodes,
        (changed_edge,) + graph.edges[1:],
    )

    assert changed.digest() != first
