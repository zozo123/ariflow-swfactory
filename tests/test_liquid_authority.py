from swfactory.liquid_authority import CONCERNS, PRIMARY, coverage, validate


def test_authority_slice_is_complete_and_bound_to_real_modules() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"authority"}
    assert {row.scheduler for row in rows} == {"airflow"}
