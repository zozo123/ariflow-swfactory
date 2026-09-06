from swfactory.liquid_airflow import CONCERNS, PRIMARY, coverage, validate


def test_airflow_slice_is_complete_and_keeps_airflow_authority() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"airflow"}
    assert {row.scheduler for row in rows} == {"airflow"}
