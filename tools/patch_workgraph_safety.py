from pathlib import Path

p = Path('src/swfactory/work_executor.py')
text = p.read_text(encoding='utf-8')

def one(old: str, new: str) -> None:
    global text
    if text.count(old) != 1:
        raise SystemExit(f'expected one match, got {text.count(old)}: {old[:100]!r}')
    text = text.replace(old, new, 1)

one('ConflictKind = Literal["disjoint", "overlap", "stale_base", "protected"]', 'ConflictKind = Literal["disjoint", "overlap", "stale_base", "protected", "undeclared"]')
one('''    final_head: str
    cancelled: bool = False
''', '''    final_head: str
    conflicts: tuple[ConflictReceipt, ...] = ()
    cancelled: bool = False
''')
one('''        declared_conflicts = conflict_set(ordered)
        parallel = (
            self.policy.allow_parallel and supports_fork and self.policy.max_parallel > 1 and not declared_conflicts
        )
        results: dict[str, NodeResult] = {}
        target_head = input_head
        merges: list[MergeReceipt] = []
''', '''        results: dict[str, NodeResult] = {}
        by_node = {node.id: node for node in ordered}
        target_head = input_head
        merges: list[MergeReceipt] = []
        conflicts: list[ConflictReceipt] = []
        used_parallel = False
''')
one('''            wave_results = (
                self._parallel(requests, cancellation)
                if parallel and len(requests) > 1
                else self._serial(requests, cancellation)
            )
''', '''            wave_parallel = (
                self.policy.allow_parallel
                and supports_fork
                and self.policy.max_parallel > 1
                and len(requests) > 1
                and all(node.parallel_safe for node in wave.nodes)
                and not conflict_set(wave.nodes)
            )
            used_parallel = used_parallel or wave_parallel
            wave_results = self._parallel(requests, cancellation) if wave_parallel else self._serial(requests, cancellation)
''')
one('''            if any(result.state != "ok" for result in wave_results):
                cancellation.cancel()
                break

            # Completion timing is irrelevant: fan-in is always node-id stable.
''', '''            if any(result.state != "ok" for result in wave_results):
                cancellation.cancel()
                break

            wave_conflicts = list(
                self.classify_conflicts(
                    wave_results,
                    observed_target_head=target_head,
                    expected_target_head=target_head,
                )
            )
            for result in wave_results:
                node = by_node[result.node_id]
                undeclared = tuple(sorted(set(result.touched_files) - set(node.files)))
                if undeclared:
                    wave_conflicts.append(
                        ConflictReceipt(result.node_id, "policy", "undeclared", undeclared, "runner touched undeclared paths")
                    )
            if wave_conflicts:
                conflicts.extend(wave_conflicts)
                cancellation.cancel()
                break

            # Completion timing is irrelevant: fan-in is always node-id stable.
''')
one('''            parallel=parallel,
            results=tuple(results[node.id] for node in ordered),
            merges=tuple(merges),
            final_head=target_head,
            cancelled=cancellation.cancelled,
''', '''            parallel=used_parallel,
            results=tuple(results[node.id] for node in ordered),
            merges=tuple(merges),
            final_head=target_head,
            conflicts=tuple(conflicts),
            cancelled=cancellation.cancelled,
''')
p.write_text(text, encoding='utf-8')

t = Path('tests/test_work_executor_safety.py')
t.write_text('''from __future__ import annotations

from swfactory.work_executor import ExecutionReport, MergeReceipt, NodeResult, WorkExecutor
from swfactory.workgraph import WorkNode


def _ok(req, touched=()):
    return NodeResult(req.logical_id, req.node.id, "ok", req.input_head, req.input_head + "-" + req.node.id, touched_files=tuple(touched))


def test_parallel_safe_false_serializes_wave() -> None:
    concurrent = {"active": 0, "max": 0}
    def runner(req):
        concurrent["active"] += 1
        concurrent["max"] = max(concurrent["max"], concurrent["active"])
        result = _ok(req, req.node.files)
        concurrent["active"] -= 1
        return result
    def merger(result, target, index):
        return MergeReceipt(index, result.node_id, result.output_head or "", target, target + "+" + result.node_id, "fast_forward", False)
    report = WorkExecutor(runner, merger).execute(
        cell_id="cell_safe", epoch=1, input_head="h0",
        nodes=(WorkNode("a", files=("a.py",), parallel_safe=True), WorkNode("b", files=("b.py",), parallel_safe=False)),
        supports_fork=True,
    )
    assert report.parallel is False
    assert concurrent["max"] == 1


def test_observed_overlap_refuses_before_merge() -> None:
    merges = []
    def runner(req):
        return _ok(req, ("shared.py",))
    def merger(result, target, index):
        merges.append(result.node_id)
        raise AssertionError("conflicted result reached merger")
    report: ExecutionReport = WorkExecutor(runner, merger).execute(
        cell_id="cell_overlap", epoch=1, input_head="h0",
        nodes=(WorkNode("a", files=("shared.py",), parallel_safe=True), WorkNode("b", files=("shared.py", "b.py"), parallel_safe=True)),
        supports_fork=True,
    )
    assert merges == []
    assert report.cancelled is True
    assert any(c.kind == "overlap" and c.files == ("shared.py",) for c in report.conflicts)


def test_undeclared_and_protected_paths_refuse_before_merge() -> None:
    merges = []
    def runner(req):
        return _ok(req, ("declared.py", "secret.txt"))
    def merger(result, target, index):
        merges.append(result.node_id)
        raise AssertionError("unsafe result reached merger")
    from swfactory.work_executor import ExecutorPolicy
    report = WorkExecutor(runner, merger, ExecutorPolicy(protected_paths=("secret.txt",))).execute(
        cell_id="cell_paths", epoch=2, input_head="h0",
        nodes=(WorkNode("a", files=("declared.py",), parallel_safe=True),), supports_fork=True,
    )
    assert merges == []
    assert {c.kind for c in report.conflicts} == {"protected", "undeclared"}
''', encoding='utf-8')
