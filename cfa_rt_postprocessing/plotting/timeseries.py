from typing import Literal

import plotly.graph_objs as go
import polars as pl


def timeseries_plot(
    state: str,
    disease: Literal["COVID-19", "Influenza"],
    obs_plot_data: pl.DataFrame,
    interval_plot_data: pl.DataFrame,
) -> go.Figure:
    """
    Read in plotting datasets and create plotly visual for single state

    Parameters
    ----------
    state: str
        string upper case abbreviation for state (EX: "US")
    disease: str
        string disease value (EX: "Influenza")
    obs_plot_data: pl
        polars dataframe for observation counts (including estimated)
    interval_plot_data: pl
        polars dataframe for estimated intervals

    Returns
    -------
    plotly fig object
    """
    df_obs = (
        obs_plot_data.filter(pl.col("geo_value") == state, pl.col("disease") == disease)
        .select(
            [
                "reference_date",
                "geo_value",
                "disease",
                "processed_obs_data",
                "raw_obs_data",
                "raw_obs_data_prev_wk",
                "expected_obs_cases",
                "expected_nowcast_cases",
            ]
        )
        .sort(["reference_date"])
    )
    df_interval = interval_plot_data.filter(
        pl.col("geo_value") == state, pl.col("disease") == disease
    ).sort(["reference_date"])

    # Line Traces
    est_current_reported_trace = go.Scatter(
        x=df_obs["reference_date"],
        y=df_obs["expected_obs_cases"],
        mode="lines",
        name="Est. Current Reported",
        line_color="black",
    )
    est_total_reported_trace = go.Scatter(
        x=df_obs["reference_date"],
        y=df_obs["expected_nowcast_cases"],
        mode="lines",
        name="Est. Total Reported",
        line=dict(dash="dash"),
        line_color="black",
    )

    # Marker Traces
    processed_reported_trace = go.Scatter(
        x=df_obs["reference_date"],
        y=df_obs["processed_obs_data"],
        mode="markers",
        name="Processed Reported",
        marker=dict(symbol="circle"),
        visible="legendonly",
    )
    raw_reported_trace = go.Scatter(
        x=df_obs["reference_date"],
        y=df_obs["raw_obs_data"],
        mode="markers",
        name="Raw Reported",
        marker=dict(symbol="circle", color="black"),
    )
    raw_prev_wk_reported_trace = go.Scatter(
        x=df_obs["reference_date"],
        y=df_obs["raw_obs_data_prev_wk"],
        mode="markers",
        name="Raw Reported (Prev Week)",
        # A red cross
        marker=dict(symbol="cross", color="rgba(255, 0, 0, 0.8)", size=5),
    )

    # Combine all traces into a list
    traces = [
        est_current_reported_trace,
        est_total_reported_trace,
        processed_reported_trace,
        raw_reported_trace,
        raw_prev_wk_reported_trace,
    ]

    # Create a figure with the collected traces
    fig = go.Figure(
        data=traces, layout=go.Layout(autosize=False, width=800, height=500)
    )

    # Loop through each width group to add ribbon traces
    for group in df_interval["_width"].unique():
        group_data = df_interval.filter(pl.col("_width") == group)
        # Add trace for the ribbon area for each group
        fig.add_trace(
            go.Scatter(
                x=group_data["reference_date"].to_list()
                + group_data["reference_date"].to_list()[::-1],
                y=group_data["_upper_expected_nowcast_cases"].to_list()
                + group_data["_lower_expected_nowcast_cases"].to_list()[::-1],
                fill="toself",
                fillcolor="rgba(75, 63, 63, 0.1)",  # light gray
                line=dict(color="rgba(151, 127, 105, 0.05)"),  # nearly white
                name="Est. Total Interval: " + str(group),
            )
        )
    # Define background color according to disease
    if disease == "COVID-19":
        fig.update_layout(
            plot_bgcolor="rgb(235, 229, 229)",  # Sets the color of the plot area
            legend=dict(
                bgcolor="rgba(226, 225, 225, 0.6)", bordercolor="Black", borderwidth=1
            ),
        )
    else:
        fig.update_layout(
            plot_bgcolor="rgba(107, 174, 214, 0.2)",  # Sets the color of the plot area
            legend=dict(
                bgcolor="rgba(107, 174, 214, 0.1)", bordercolor="Black", borderwidth=1
            ),
        )

    # Update the title
    fig.update_layout(title=state + ": " + disease + " ED Visits")
    # Update the x-axis title
    fig.update_xaxes(title_text="Reference Date")
    # Update the y-axis title
    fig.update_yaxes(title_text="Incident ED Visits")

    return fig
