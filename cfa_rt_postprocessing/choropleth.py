from random import choices

import altair as alt
from vega_datasets import data
import pandas as pl


def create_choropleth(p_growing: pl.DataFrame, states: alt.ChartDataType) -> alt.Chart:
    """
    Create a choropleth map of the US states with the probability of growing.
    Return the altair chart object.
    """
    return (
        alt.Chart(states)
        .mark_geoshape(stroke="white", strokeWidth=1.5)
        .project("albersUsa")
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                p_growing,
                "id",
                p_growing.columns.tolist(),
            ),
        )
        .encode(
            color=alt.Color(
                "p_growing:Q",
                scale=alt.Scale(scheme="cividis"),
            ).legend(title="P(growing)"),
            tooltip=[
                alt.Tooltip("state:N", title="State"),
                alt.Tooltip("p_growing:Q", title="P(growing)"),
            ],
        )
        .interactive()
        .properties(width=1_000, height=1_000)
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


def five_cat_hexcolor() -> dict[str, str]:
    return {
        "Growing": "#6d085a",
        "Likely Growing": "#b83d93",
        "Not Changing": "#bdbdbd",
        "Likely Declining": "#3bbbb0",
        "Declining": "#006166",
        "Not Estimated": "#ffffff",
    }


if __name__ == "__main__":
    states = alt.topo_feature(url=data.us_10m.url, feature="states")

    # Create some fake p_growing data
    N = 60
    p_growing = pl.DataFrame(
        dict(
            id=range(0, N),
            p_growing=choices(population=[1 / (x + 1) for x in range(5)], k=N),
        )
    )
    create_choropleth(p_growing, states).save("choropleth.html")
