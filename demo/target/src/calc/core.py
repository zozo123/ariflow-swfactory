"""Core interest calculations."""


def simple_interest(principal: float, rate: float, years: float) -> float:
    """Return principal * rate * years. Rate is a fraction (0.05 == 5%)."""
    if principal < 0 or rate < 0 or years < 0:
        raise ValueError("principal, rate and years must be non-negative")
    return principal * rate * years


def compound(principal: float, rate: float, years: int, periods_per_year: int = 1) -> float:
    """Return the final amount after compounding. Rate is a fraction."""
    if principal < 0 or rate < 0 or years < 0 or periods_per_year < 1:
        raise ValueError("invalid arguments")
    n = periods_per_year
    return principal * (1 + rate / n) ** (n * years)


def percent_change(old: float, new: float) -> float:
    """Return (new - old) / old, the relative change as a fraction (0.25 == +25%)."""
    if old == 0:
        raise ValueError("old must be non-zero")
    return (new - old) / old
