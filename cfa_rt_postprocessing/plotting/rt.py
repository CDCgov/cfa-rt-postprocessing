import matplotlib.axes
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.dates
import polars as pl


def rt(
    summary: pl.DataFrame, state: str, disease: str, ax=None
) -> matplotlib.axes.Axes:
    """
    Plot the Rt 95% width Rt estimates as a band plot, with the median as a line in the
    middle.
    """
    # If an axis was not handed in, create one
    if ax is None:
        fig, ax = plt.subplots()
    ax.set_title(f"{state}-{disease} Rt estimates")
    ax.set_ylabel("Rt")
    ax.set_xlabel("Date")

    # Filter the summary to the state, and Rt variables
    df = summary.filter(
        pl.col.geo_value.eq(state),
        pl.col.disease.eq(disease),
        pl.col("_variable").eq("Rt"),
    )

    # Plot the median Rt estimates
    # Median is stored in the `value` column, and has duplicates for each quantile
    med = df.filter(pl.col("_width").eq(0.5)).select(["value", "reference_date"])
    ax.plot(med["reference_date"], med["value"], label="Median Rt")

    # Plot the 95% width of the Rt estimates
    # The 95% width has values stored in _lower and _upper columns
    # The reference_date is the same for both columns
    width_95 = df.filter(pl.col("_width").eq(0.95)).select(
        ["_lower", "_upper", "reference_date"]
    )
    ax.fill_between(
        width_95["reference_date"],
        width_95["_lower"],
        width_95["_upper"],
        alpha=0.20,
        label="95% width",
        color="tab:blue",
    )

    # Plot the 50% width of the Rt estimates
    width_50 = df.filter(pl.col("_width").eq(0.5)).select(
        ["_lower", "_upper", "reference_date"]
    )
    ax.fill_between(
        width_50["reference_date"],
        width_50["_lower"],
        width_50["_upper"],
        alpha=0.25,
        label="50% width",
        color="tab:blue",
    )

    # Change the date format to be Month name. Day
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %d"))

    # Add a legend
    ax.legend()

    return ax
