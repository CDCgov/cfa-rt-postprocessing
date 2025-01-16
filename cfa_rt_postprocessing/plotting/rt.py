import altair as alt
import polars as pl


def plot_rt(summary: pl.DataFrame, state: str, disease: str) -> alt.LayerChart:
    """
    Plot the Rt 95% width Rt estimates as a band plot, with the median as a line in the
    middle.
    """
    # Filter the summary to the state, and Rt variables
    df = summary.filter(
        pl.col.geo_value.eq(state),
        pl.col.disease.eq(disease),
        pl.col("_variable").eq("Rt"),
    )

    # Define an Altair color scale
    color_scale = alt.Scale(
        domain=["95% Width", "50% Width", "Median"],
        range=["#1F77B4", "#1F77B4", "#1F77B4"],
    )

    # Plot the median Rt estimates
    # Median is stored in the `value` column, and has duplicates for each quantile
    med = (
        df.filter(pl.col("_width").eq(0.5))
        .select(["value", "reference_date"])
        .with_columns(label=pl.lit("Median"))
    )
    med_line = (
        alt.Chart(med, title=f"{state}-{disease} Rt estimates")
        .mark_line(strokeWidth=4)
        .encode(
            x=alt.X("reference_date:T").title("Date"),
            y=alt.Y("value:Q").title("Rt").scale(zero=False),
            color=alt.Color("label:N").scale(color_scale),
        )
    )

    # Plot the 95% width of the Rt estimates
    # The 95% width has values stored in _lower and _upper columns
    # The reference_date is the same for both columns
    width_95 = (
        df.filter(pl.col("_width").eq(0.95))
        .select(["_lower", "_upper", "reference_date"])
        .with_columns(label=pl.lit("95% Width"))
    )
    width_95_band = (
        alt.Chart(width_95)
        .mark_errorband(opacity=0.20, color="blue")
        .encode(
            x="reference_date:T",
            y=alt.Y("_lower:Q").title("").scale(zero=False),
            y2="_upper:Q",
            color=alt.Color("label:N").scale(color_scale),
        )
    )

    # Plot the 50% width of the Rt estimates
    width_50 = (
        df.filter(pl.col("_width").eq(0.5))
        .select(["_lower", "_upper", "reference_date"])
        .with_columns(label=pl.lit("50% Width"))
    )
    width_50_band = (
        alt.Chart(width_50)
        .mark_errorband(opacity=0.25, color="blue")
        .encode(
            alt.X("reference_date:T"),
            y=alt.Y("_lower:Q").title(""),
            y2="_upper:Q",
            color=alt.Color("label:N").scale(color_scale).scale(zero=False),
        )
    )

    # Create a black line at Rt = 1
    line = (
        alt.Chart(data=pl.DataFrame({"y": [1]}))
        .mark_rule(color="black")
        .encode(y="y:Q")
    )

    # Combine the plots
    return line + width_95_band + width_50_band + med_line
