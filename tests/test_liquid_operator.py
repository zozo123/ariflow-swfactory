from swfactory.liquid_operator import CONCERNS, PRIMARY, coverage, validate


def test_operator_slice_is_complete_and_parity_bound() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 40
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"operator"}
    assert {row.scheduler for row in rows} == {"airflow"}
