import altair as alt
import numpy as np
from vega_datasets import data
import polars as pl


def create_choropleth(p_growing: pl.DataFrame, states: alt.ChartDataType) -> alt.Chart:
    """
    Create a choropleth map of the US states with the probability of growing.
    Return the altair chart object.
    """
    # Bin `p_growing` into 5 categories, and assign a color to each category
    categories = five_cat_hexcolor()
    df = p_growing.join_asof(
        categories,
        left_on="p_growing",
        right_on="upper_threshold",
        strategy="forward",
    )
    cats = dict(zip(categories["cat"], categories["hex"]))

    return (
        alt.Chart(states)
        .mark_geoshape(stroke="white", strokeWidth=1.5)
        .project("albersUsa")
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                df,
                "id",
                df.columns,
            ),
        )
        .encode(
            color=alt.Color("cat:N")
            .scale(domain=cats.keys(), range=cats.values())
            .legend(title="P(growing)", orient="right"),
            tooltip=[
                alt.Tooltip("state:N", title="State"),
                alt.Tooltip("p_growing:Q", title="P(growing)"),
            ],
        )
        .interactive()
        .properties(width=750, height=750)
    )


def states() -> list[str]:
    return [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]


def five_cat_hexcolor() -> pl.DataFrame:
    df = pl.DataFrame(
        dict(
            cat=[
                "Growing",
                "Likely Growing",
                "Not Changing",
                "Likely Declining",
                "Declining",
                "Not Estimated",
            ],
            hex=["#6d085a", "#b83d93", "#bdbdbd", "#3bbbb0", "#006166", "#ffffff"],
            lower_threshold=[0.9, 0.75, 0.25, 0.10, 0.0, None],
            upper_threshold=[1.0, 0.9, 0.75, 0.25, 0.10, None],
        )
    ).sort("upper_threshold")
    return df


if __name__ == "__main__":
    states_data = alt.topo_feature(url=data.us_10m.url, feature="states")

    # Create some fake p_growing data
    N = 60
    p_growing = pl.DataFrame(
        dict(
            id=range(0, N),
            state=np.random.choice(states(), N),
            p_growing=np.random.rand(N),
        )
    ).sort("p_growing")
    create_choropleth(p_growing, states_data).save("choropleth.html")
