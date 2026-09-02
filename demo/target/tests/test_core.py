import pytest

from calc import compound, simple_interest


def test_simple_interest():
    assert simple_interest(1000, 0.05, 2) == pytest.approx(100.0)


def test_compound_annual():
    assert compound(1000, 0.10, 2) == pytest.approx(1210.0)


def test_negative_rejected():
    with pytest.raises(ValueError):
        simple_interest(-1, 0.1, 1)
