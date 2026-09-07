from swfactory.liquid_recovery import CONCERNS, PRIMARY, coverage, validate


def test_recovery_slice_is_complete_and_repairable() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"recovery"}
    assert {row.scheduler for row in rows} == {"airflow"}
