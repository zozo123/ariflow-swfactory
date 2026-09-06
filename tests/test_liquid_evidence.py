from swfactory.liquid_evidence import CONCERNS, PRIMARY, coverage, validate


def test_evidence_slice_is_complete_and_retained() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"evidence"}
    assert {row.scheduler for row in rows} == {"airflow"}
