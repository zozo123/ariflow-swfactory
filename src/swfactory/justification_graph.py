"""Typed, content-addressed justification hypergraph for one Formal Quench.

The graph is a provenance/checking substrate, not a universal theorem prover.  Semantic validity
belongs to the verifier that issues an evidence node or derivation edge; this module checks that
those objects are bound to one frozen quench, form an acyclic derivation, and cannot self-assert
trust.

A conclusion may depend jointly on several premises, so the primitive is a hyperedge.  Its ordinary
dependency projection is a DAG.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum

JUSTIFICATION_GRAPH_SCHEMA_VERSION = 1
JUSTIFICATION_GRAPH_AUTHORITY = "evidence-only"
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class JustificationGraphError(ValueError):
    """The justification graph is malformed or crosses a frozen boundary."""


class NodeKind(StrEnum):
    ARTIFACT = "artifact"
    ASSUMPTION = "assumption"
    MODEL = "model"
    CLAIM = "claim"
    EVIDENCE = "evidence"
    COUNTEREXAMPLE = "counterexample"
    REFINEMENT = "refinement"


class EdgeKind(StrEnum):
    SUPPORT = "support"
    INFERENCE = "inference"
    REFINEMENT = "refinement"
    REFUTATION = "refutation"


def _require_digest(value: str, field: str) -> None:
    if not _SHA256.fullmatch(value):
        raise JustificationGraphError(f"{field} must be sha256:<64 lowercase hex characters>")


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class JustificationNode:
    quench_digest: str
    kind: NodeKind
    statement: str
    payload_digest: str
    claim_id: str | None = None
    issuer: str | None = None

    def validate(self) -> None:
        _require_digest(self.quench_digest, "quench_digest")
        _require_digest(self.payload_digest, "payload_digest")
        if not self.statement.strip():
            raise JustificationGraphError("node statement must be nonempty")
        if self.kind in {NodeKind.CLAIM, NodeKind.REFINEMENT}:
            if self.claim_id is None or not self.claim_id.strip():
                raise JustificationGraphError(f"{self.kind.value} nodes require a claim_id")
        elif self.claim_id is not None:
            raise JustificationGraphError(f"{self.kind.value} nodes may not declare a claim_id")
        if self.kind in {NodeKind.EVIDENCE, NodeKind.COUNTEREXAMPLE}:
            if self.issuer is None or not self.issuer.strip():
                raise JustificationGraphError(f"{self.kind.value} nodes require an issuer")
        elif self.issuer is not None:
            raise JustificationGraphError(f"{self.kind.value} nodes may not self-declare an issuer")

    def digest(self) -> str:
        self.validate()
        return _digest(
            {
                "schema_version": JUSTIFICATION_GRAPH_SCHEMA_VERSION,
                "quench_digest": self.quench_digest,
                "kind": self.kind.value,
                "statement": self.statement,
                "payload_digest": self.payload_digest,
                "claim_id": self.claim_id,
                "issuer": self.issuer,
            }
        )


@dataclass(frozen=True)
class JustificationEdge:
    quench_digest: str
    kind: EdgeKind
    rule: str
    premises: tuple[str, ...]
    conclusion: str
    verifier: str
    receipt_digest: str

    def validate(self) -> None:
        _require_digest(self.quench_digest, "quench_digest")
        _require_digest(self.conclusion, "conclusion")
        _require_digest(self.receipt_digest, "receipt_digest")
        if not self.rule.strip():
            raise JustificationGraphError("edge rule must be nonempty")
        if not self.verifier.strip():
            raise JustificationGraphError("edge verifier must be nonempty")
        if not self.premises:
            raise JustificationGraphError("a derivation edge requires at least one premise")
        for premise in self.premises:
            _require_digest(premise, "premise")
        if len(set(self.premises)) != len(self.premises):
            raise JustificationGraphError("a derivation edge may not repeat a premise")

    def digest(self) -> str:
        self.validate()
        return _digest(
            {
                "schema_version": JUSTIFICATION_GRAPH_SCHEMA_VERSION,
                "quench_digest": self.quench_digest,
                "kind": self.kind.value,
                "rule": self.rule,
                "premises": list(self.premises),
                "conclusion": self.conclusion,
                "verifier": self.verifier,
                "receipt_digest": self.receipt_digest,
            }
        )


@dataclass(frozen=True)
class JustificationProjection:
    quench_digest: str
    graph_digest: str
    root_node: str
    root_claim_id: str
    root_supported: bool
    root_refuted: bool
    supported_claims: tuple[str, ...]
    refuted_claims: tuple[str, ...]
    ignored_objects: tuple[str, ...]
    authority: str = JUSTIFICATION_GRAPH_AUTHORITY

    @property
    def justified(self) -> bool:
        return self.root_supported and not self.root_refuted


@dataclass(frozen=True)
class JustificationGraph:
    quench_digest: str
    nodes: tuple[JustificationNode, ...]
    edges: tuple[JustificationEdge, ...]

    def validate(self) -> None:
        _require_digest(self.quench_digest, "quench_digest")
        if not self.nodes:
            raise JustificationGraphError("justification graph requires at least one node")

        node_map: dict[str, JustificationNode] = {}
        claim_ids: set[str] = set()
        for node in self.nodes:
            node.validate()
            if node.quench_digest != self.quench_digest:
                raise JustificationGraphError("node belongs to a different Formal Quench")
            ident = node.digest()
            if ident in node_map:
                raise JustificationGraphError("duplicate justification node")
            if node.kind == NodeKind.CLAIM:
                assert node.claim_id is not None
                if node.claim_id in claim_ids:
                    raise JustificationGraphError("duplicate claim_id in justification graph")
                claim_ids.add(node.claim_id)
            node_map[ident] = node

        edge_ids: set[str] = set()
        adjacency: dict[str, set[str]] = {ident: set() for ident in node_map}
        indegree: dict[str, int] = {ident: 0 for ident in node_map}

        for edge in self.edges:
            edge.validate()
            if edge.quench_digest != self.quench_digest:
                raise JustificationGraphError("edge belongs to a different Formal Quench")
            ident = edge.digest()
            if ident in edge_ids:
                raise JustificationGraphError("duplicate justification edge")
            edge_ids.add(ident)
            if edge.conclusion not in node_map:
                raise JustificationGraphError("edge conclusion is not a graph node")
            if any(premise not in node_map for premise in edge.premises):
                raise JustificationGraphError("edge premise is not a graph node")
            conclusion_kind = node_map[edge.conclusion].kind
            if conclusion_kind not in {NodeKind.CLAIM, NodeKind.REFINEMENT}:
                raise JustificationGraphError("derivations may conclude only claims or refinements")
            if edge.kind == EdgeKind.REFUTATION and conclusion_kind != NodeKind.CLAIM:
                raise JustificationGraphError("refutation must target a claim")
            for premise in edge.premises:
                if edge.conclusion not in adjacency[premise]:
                    adjacency[premise].add(edge.conclusion)
                    indegree[edge.conclusion] += 1

        ready = sorted(ident for ident, degree in indegree.items() if degree == 0)
        visited = 0
        while ready:
            ident = ready.pop()
            visited += 1
            for child in sorted(adjacency[ident]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        if visited != len(node_map):
            raise JustificationGraphError("justification graph must be acyclic")

    def node_map(self) -> dict[str, JustificationNode]:
        self.validate()
        return {node.digest(): node for node in self.nodes}

    def digest(self) -> str:
        self.validate()
        return _digest(
            {
                "schema_version": JUSTIFICATION_GRAPH_SCHEMA_VERSION,
                "quench_digest": self.quench_digest,
                "nodes": sorted(node.digest() for node in self.nodes),
                "edges": sorted(edge.digest() for edge in self.edges),
            }
        )

    def project(
        self,
        *,
        root_node: str,
        trusted_verifiers: frozenset[str],
    ) -> JustificationProjection:
        """Project trusted support/refutation without pretending to re-check verifier semantics."""

        self.validate()
        nodes = self.node_map()
        if root_node not in nodes:
            raise JustificationGraphError("root_node is not present in the graph")
        if nodes[root_node].kind != NodeKind.CLAIM:
            raise JustificationGraphError("root_node must be a claim")
        if any(not verifier.strip() for verifier in trusted_verifiers):
            raise JustificationGraphError("trusted verifier identities must be nonempty")

        incoming: dict[str, list[JustificationEdge]] = {ident: [] for ident in nodes}
        ignored: set[str] = {
            node.payload_digest
            for node in nodes.values()
            if node.kind in {NodeKind.EVIDENCE, NodeKind.COUNTEREXAMPLE} and node.issuer not in trusted_verifiers
        }
        for edge in self.edges:
            incoming[edge.conclusion].append(edge)
            if edge.verifier not in trusted_verifiers:
                ignored.add(edge.receipt_digest)

        support_memo: dict[str, bool] = {}
        refute_memo: dict[str, bool] = {}

        def refuted(node_id: str) -> bool:
            if node_id in refute_memo:
                return refute_memo[node_id]
            result = any(
                edge.verifier in trusted_verifiers
                and edge.kind == EdgeKind.REFUTATION
                and all(admitted(premise) for premise in edge.premises)
                for edge in incoming[node_id]
            )
            refute_memo[node_id] = result
            return result

        def admitted(node_id: str) -> bool:
            if node_id in support_memo:
                return support_memo[node_id]
            node = nodes[node_id]
            if node.kind in {NodeKind.ARTIFACT, NodeKind.ASSUMPTION, NodeKind.MODEL}:
                support_memo[node_id] = True
                return True
            if node.kind in {NodeKind.EVIDENCE, NodeKind.COUNTEREXAMPLE}:
                result = node.issuer in trusted_verifiers
                support_memo[node_id] = result
                return result

            supporting = [
                edge
                for edge in incoming[node_id]
                if edge.kind != EdgeKind.REFUTATION and edge.verifier in trusted_verifiers
            ]
            has_support = any(all(admitted(premise) for premise in edge.premises) for edge in supporting)
            result = has_support and not refuted(node_id)
            support_memo[node_id] = result
            return result

        supported_claims: list[str] = []
        refuted_claims: list[str] = []
        for ident, node in sorted(nodes.items()):
            if node.kind != NodeKind.CLAIM:
                continue
            is_refuted = refuted(ident)
            if is_refuted:
                refuted_claims.append(node.claim_id or ident)
            elif admitted(ident):
                supported_claims.append(node.claim_id or ident)

        root_refuted = refuted(root_node)
        root_supported = admitted(root_node) and not root_refuted
        return JustificationProjection(
            quench_digest=self.quench_digest,
            graph_digest=self.digest(),
            root_node=root_node,
            root_claim_id=nodes[root_node].claim_id or root_node,
            root_supported=root_supported,
            root_refuted=root_refuted,
            supported_claims=tuple(supported_claims),
            refuted_claims=tuple(refuted_claims),
            ignored_objects=tuple(sorted(ignored)),
        )
