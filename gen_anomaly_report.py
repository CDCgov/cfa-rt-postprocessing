from typing import Literal

import polars as pl
import altair as alt


def nowcast_data_plot(
    disease: Literal["covid", "flu"],
    geo_value: str,
    report: pl.DataFrame,
    old_report: pl.DataFrame,
    model_summary: pl.DataFrame,
    output_filename: str,
):
    # Plot the reported data as black dots
    rep_plot = (
        alt.Chart(
            report.filter(
                pl.col.metric.eq("count_ed_visits"),
                pl.col.disease.eq(disease),
                pl.col.geo_value.eq(geo_value),
            )
        )
        .mark_point(color="black")
        .encode(x="reference_date:T", y="value:Q")
    )

    # Add the old reported data as red pluses
    old_rep_plot = (
        alt.Chart(
            old_report.filter(
                pl.col.metric.eq("count_ed_visits"),
                pl.col.disease.eq(disease),
                pl.col.geo_value.eq(geo_value),
            )
        )
        .mark_point(color="red", shape="cross")
        .encode(x="reference_date:T", y="value:Q")
    )

    # Add the model's expected_obs_cases as a solid black line
    model_plot = (
        alt.Chart(
            model_summary.filter(
                pl.col("_variable").eq("expected_obs_cases"),
                pl.col("_width").eq(0.50),
                pl.col.disease.eq(disease),
                pl.col.geo_value.eq(geo_value),
            )
        )
        .mark_line(color="black")
        .encode(x="reference_date:T", y="value:Q")
    )

    # Add the model's expected_nowcast_cases as a dashed black line
    nowcast_plot = (
        alt.Chart(
            model_summary.filter(
                pl.col("_variable").eq("expected_nowcast_cases"),
                pl.col._width.eq(0.50),
                pl.col.disease.eq(disease),
                pl.col.geo_value.eq(geo_value),
            )
        )
        .mark_line(color="black", strokeDash=[5, 5])
        .encode(x="reference_date:T", y="value:Q")
    )

    # Add the 50% and 95% percentile intervals as shaded areas
    interval_plot = (
        alt.Chart(
            model_summary.filter(
                pl.col("_variable").eq("expected_obs_cases"),
                pl.col.disease.eq(disease),
                pl.col.geo_value.eq(geo_value),
            )
        )
        .mark_area(opacity=0.3)
        .encode(
            x="reference_date:T",
            y="_lower:Q",
            y2="_upper:Q",
        )
    )
