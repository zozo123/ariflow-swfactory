from swfactory.liquid_workgraph import CONCERNS, PRIMARY, coverage, validate


def test_workgraph_slice_is_complete_and_bound_to_runtime() -> None:
    rows = coverage()
    assert len(rows) == len(PRIMARY) * len(CONCERNS) == 60
    assert validate() == ()
    assert {row.owner_role for row in rows} == {"workgraph"}
    assert {row.scheduler for row in rows} == {"airflow"}
