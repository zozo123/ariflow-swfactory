from __future__ import annotations

import pytest

from swfactory.crash_conformance import run_crash_cycle


@pytest.mark.parametrize("cycle", range(64))
def test_crash_takeover_is_durable_and_fenced(tmp_path, cycle: int) -> None:
    result = run_crash_cycle(tmp_path, cycle)
    assert result.old_epoch == 1
    assert result.new_epoch == 2
    assert result.stale_writer_fenced is True
    assert result.ambiguous_observed is True
    assert result.current_write_calls == 1
    assert result.history_events >= 4
