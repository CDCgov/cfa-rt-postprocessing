from datetime import date

import polars as pl
import polars.testing
import pytest

from src.cfa_rt_postprocessing.main_functions import update_production_index


def test_schema_and_types():
    # Define the schema and lazy frame
    schema = pl.Schema(
        [
            ("release_date", pl.Date),
            ("run_date", pl.Date),
        ]
    )
    lf = pl.LazyFrame(schema=schema)

    output_lf: pl.LazyFrame = update_production_index(
        production_index=lf,
        release_date=date(2025, 1, 31),
        run_date=date(2025, 1, 29),
    )

    # This will raise an error if the type checking fails
    output_lf.collect()

    # The desired output schema
    desired_schema: pl.Schema = schema.copy()

    # Compare with what we got. Use the .collect_schema() method to get past
    # the warning
    assert output_lf.collect_schema() == desired_schema


@pytest.fixture
def production_index() -> pl.LazyFrame:
    # Use release_date of the last week in January 2025
    prod_weeks = [date(2025, 1, 31)]
    # Use run_date of the last Wednesday in January 2025
    prod_dates = [date(2025, 1, 29)]

    lf = pl.LazyFrame(dict(release_date=prod_weeks, run_date=prod_dates))
    return lf


params = [
    # Test updating from a Wednesdsay run to a Thursday run
    (
        production_index,
        date(2025, 1, 31),
        date(2025, 1, 30),
        pl.DataFrame(
            dict(release_date=[date(2025, 1, 31)], run_date=[date(2025, 1, 30)])
        ),
    ),
    # Test updating from a Wednesdsay run to a Friday run
    (
        production_index,
        date(2025, 1, 31),
        date(2025, 1, 31),
        pl.DataFrame(
            dict(release_date=[date(2025, 1, 31)], run_date=[date(2025, 1, 31)])
        ),
    ),
    # Test adding a new week
    (
        production_index,
        date(2025, 2, 7),
        date(2025, 2, 5),
        pl.DataFrame(
            dict(
                release_date=[date(2025, 1, 31), date(2025, 2, 7)],
                run_date=[date(2025, 1, 29), date(2025, 2, 5)],
            )
        ),
    ),
]


@pytest.mark.parametrize("prod_idx, prod_week, prod_date, want", params)
def test_update_production_index(
    prod_idx: pl.LazyFrame,
    prod_week: date,
    prod_date: date,
    want: pl.DataFrame,
    request: pytest.FixtureRequest,
):
    prod_idx = request.getfixturevalue("production_index")
    got = update_production_index(prod_idx, prod_week, prod_date).collect()
    polars.testing.assert_frame_equal(want, got)
