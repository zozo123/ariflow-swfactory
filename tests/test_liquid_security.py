from swfactory.liquid_security import CONCERNS, PRIMARY, coverage, validate


def test_security_slice_is_complete_and_least_privilege_bound() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"security"}
    assert {row.scheduler for row in rows} == {"airflow"}
