from datetime import date
import pytest

from src.cfa_rt_postprocessing.main_functions import round_up_to_friday


@pytest.mark.parametrize(
    "in_date, want_date",
    [
        (date(2025, 1, 25), date(2025, 1, 31)),
        (date(2025, 1, 26), date(2025, 1, 31)),
        (date(2025, 1, 27), date(2025, 1, 31)),
        (date(2025, 1, 28), date(2025, 1, 31)),
        (date(2025, 1, 29), date(2025, 1, 31)),
        (date(2025, 1, 30), date(2025, 1, 31)),
        (date(2025, 1, 31), date(2025, 1, 31)),
        (date(2025, 2, 1), date(2025, 2, 7)),
        (date(2025, 2, 2), date(2025, 2, 7)),
        (date(2025, 2, 3), date(2025, 2, 7)),
        (date(2025, 2, 4), date(2025, 2, 7)),
        (date(2025, 2, 5), date(2025, 2, 7)),
        (date(2025, 2, 6), date(2025, 2, 7)),
        (date(2025, 2, 7), date(2025, 2, 7)),
        (date(2025, 2, 8), date(2025, 2, 14)),
    ],
)
def test_round_up_to_friday(in_date: date, want_date: date):
    got_date = round_up_to_friday(in_date)
    assert got_date == want_date
