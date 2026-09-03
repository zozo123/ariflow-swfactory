import pytest

import calc
from calc import compound, percent_change, simple_interest


def test_simple_interest():
    assert simple_interest(1000, 0.05, 2) == pytest.approx(100.0)


def test_compound_annual():
    assert compound(1000, 0.10, 2) == pytest.approx(1210.0)


def test_negative_rejected():
    with pytest.raises(ValueError):
        simple_interest(-1, 0.1, 1)


def test_percent_change_rise():
    assert percent_change(100, 125) == pytest.approx(0.25)


def test_percent_change_fall():
    assert percent_change(200, 150) == pytest.approx(-0.25)


def test_percent_change_no_change():
    assert percent_change(50, 50) == pytest.approx(0.0)


@pytest.mark.parametrize("old", [0, 0.0, -0.0])
def test_percent_change_zero_old_rejected(old):
    with pytest.raises(ValueError):
        percent_change(old, 10)


def test_percent_change_negative_old():
    assert percent_change(-100, -50) == pytest.approx(-0.5)


def test_percent_change_to_zero():
    assert percent_change(100, 0) == pytest.approx(-1.0)


def test_percent_change_returns_float():
    result = percent_change(1, 2)
    assert isinstance(result, float)
    assert result == pytest.approx(1.0)


def test_percent_change_exported():
    assert "percent_change" in calc.__all__


def test_percent_change_docstring():
    assert percent_change.__doc__
    assert "fraction" in percent_change.__doc__
