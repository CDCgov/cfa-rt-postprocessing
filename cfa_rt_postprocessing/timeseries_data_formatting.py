from datetime import date, timedelta
from io import BytesIO

import polars as pl
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient


# Reads files straight from blob (not read a local downloaded version)
def read_blob_file(
    container_name: str, blob_file_path: str, blob_service_client: BlobServiceClient
):
    """
    Reads a single parquet file from blob into memory

    Parameters
    ----------
    container_name: str
        name of the container (EX: "nssp-rt-post-process")
    blob_file_path: str
        name of the blob  (EX: "2025-01-17/internal-review/summaries.parquet")
    blob_service_client: BlobServiceClient
        BlobServiceClient object

    Returns
    -------
    polars.dataframe
        File read from blob
    """
    input_container: ContainerClient = blob_service_client.get_container_client(
        container_name
    )
    blob_client = input_container.get_blob_client(blob_file_path)
    download_stream = blob_client.download_blob()
    content = download_stream.readall()  # Read all bytes of the blob
    bytes_io = BytesIO(content)
    df = pl.read_parquet(bytes_io).with_columns(pl.col(pl.Categorical).cast(pl.String))
    return df


def gold_data_formatting(gold_df: pl, value_name: str, date_cut_off: date):
    """
    Function to format a nssp gold dataset (rename columns, cast to string, select diseases, etc)

    Parameters
    ----------
    gold_df : pl
        gold data as polar dataframe
    value_name: str
        what to rename the value column to (EX: "raw_data versus" "raw_data_prev_wk")
    date_cut_off: date
        min date for plots to come (8 weeks before report_date)

    Returns
    -------
    polars.dataframe
        processed gold data
    """
    nssp_gold = (
        gold_df.filter(pl.col("metric") == "count_ed_visits")
        .with_columns(
            [
                (
                    pl.when(pl.col("disease") == "COVID-19/Omicron")
                    .then(pl.lit("COVID-19"))
                    .otherwise(pl.col("disease"))
                )
                .cast(pl.String)
                .name.keep(),
                pl.col("geo_value").cast(pl.String),
            ]
        )
        .filter(
            (pl.col("disease") == "COVID-19")
            | (pl.col("disease") == "Influenza"),  # Filter based on pathogen_to_run
            pl.col("reference_date") >= date_cut_off,
        )
    )
    # Summarize health data by reference_date, geo_value, and disease
    healthdata_df_agg = (
        nssp_gold.group_by(["reference_date", "geo_value", "disease"])
        .agg(pl.col("value").sum().alias("value"))
        .rename({"value": value_name})
    )
    return healthdata_df_agg


def combine_gold_current_and_prev(
    date_to_use: date, blob_service_client: BlobServiceClient
):
    """
    Read in current and previous gold files with read_blob_file(). Format each with gold_data_formatting(). Merge them together, create an aggregate US version. Merge back together

    Parameters
    ----------
    blob_service_client: BlobServiceClient
        BlobServiceClient object
    date_to_use: date
        report date

    Returns
    -------
    polars.dataframe
        merged gold data (current date and week prior)
    """
    past_week = date_to_use - timedelta(days=7)
    date_cut_off = date_to_use - timedelta(weeks=8)
    # read the gold files (same date as summaries.parquet and week prior)
    nssp_gold = read_blob_file(
        blob_service_client=blob_service_client,
        container_name="nssp-etl",
        blob_file_path="gold/" + date_to_use.strftime("%Y-%m-%d") + ".parquet",
    )
    nssp_gold_previous_wk = read_blob_file(
        blob_service_client=blob_service_client,
        container_name="nssp-etl",
        blob_file_path="gold/" + past_week.strftime("%Y-%m-%d") + ".parquet",
    )
    healthdata_df_agg = gold_data_formatting(
        gold_df=nssp_gold, value_name="raw_obs_data", date_cut_off=date_cut_off
    )
    healthdata_df_agg_prev_week = gold_data_formatting(
        gold_df=nssp_gold_previous_wk,
        value_name="raw_obs_data_prev_wk",
        date_cut_off=date_cut_off,
    )
    healthdata_df_agg_join = healthdata_df_agg.select(
        ["reference_date", "geo_value", "disease", "raw_obs_data"]
    ).join(
        healthdata_df_agg_prev_week.select(
            ["reference_date", "geo_value", "disease", "raw_obs_data_prev_wk"]
        ),
        on=["reference_date", "geo_value", "disease"],
        how="left",
    )
    # Create US data by summarizing across all states and then adding 'US' as a state
    us_df_agg = (
        healthdata_df_agg_join.filter(
            pl.col("geo_value") != "US"
        )  # Exclude existing 'US' rows if any
        .with_columns(pl.lit("US").alias("geo_value"))
        .group_by(["reference_date", "geo_value", "disease"])
        .agg(
            pl.col("raw_obs_data").sum().alias("raw_obs_data"),
            pl.col("raw_obs_data_prev_wk").sum().alias("raw_obs_data_prev_wk"),
        )
        # .select(["reference_date","geo_value","disease","value"])
    )
    # Combine health data with US data
    healthdata_df_combined = pl.concat([healthdata_df_agg_join, us_df_agg])
    return healthdata_df_combined


def read_and_process_summary_data(
    date_to_use: date,
    blob_service_client: BlobServiceClient,
    variable_values: tuple = (
        "processed_obs_data",
        "expected_nowcast_cases",
        "expected_obs_cases",
    ),
):
    """
    Read summaries.parquet with read_blob_file() and then reformat and pivot the data

    Parameters
    ----------
    blob_service_client: BlobServiceClient
        BlobServiceClient object
    date_to_use: date
        report date
    variable_values: tuple
        list of the _variable values to pivot over into columns

    Returns
    -------
    polars.dataframe
        processed and pivoted summaries data
    """
    # Read the summaries parquet file
    summary_df = read_blob_file(
        blob_service_client=blob_service_client,
        container_name="nssp-rt-post-process",
        blob_file_path=date_to_use.strftime("%Y-%m-%d")
        + "/internal-review/summaries.parquet",
    )
    summary_pivot = (
        summary_df.filter(pl.col("_variable").is_in(variable_values))
        .pivot(
            on="_variable",
            values=["value", "_lower", "_upper"],
            index=["time", "reference_date", "geo_value", "disease", "_width"],
        )
        .drop(["_upper_processed_obs_data", "_lower_processed_obs_data"])
        .rename(
            {
                "value_processed_obs_data": "processed_obs_data",
                "value_expected_obs_cases": "expected_obs_cases",
                "value_expected_nowcast_cases": "expected_nowcast_cases",
            }
        )
        .sort(["disease", "geo_value", "reference_date"])
    )
    return summary_pivot


def process_obs_plot_data(merged_gold_dfs: pl, summary_df: pl):
    """
    Create observation dataframe using both gold and summary datasets

    Parameters
    ----------
    merged_gold_dfs: pl
        merged gold data (today's + last week's)
    summary_df: pl
        reformatted summary data

    Returns
    -------
    polars.dataframe
        observation data from both gold and summaries
    """
    raw_processed_obs = merged_gold_dfs.join(
        (
            summary_df.group_by(["time", "reference_date", "geo_value", "disease"]).agg(
                [
                    pl.max("processed_obs_data"),
                    pl.max("expected_obs_cases"),
                    pl.max("expected_nowcast_cases"),
                ]
            )
        ),
        on=["reference_date", "geo_value", "disease"],
        how="left",
    )
    return raw_processed_obs


def process_interval_plot_data(raw_processed_obs: pl, summary_df: pl):
    """
    Create dataset with interval data using raw_processed_obs and summary_df

    Parameters
    ----------
    raw_processed_obs: pl
        processed observation data
    summary_df: pl
        reformatted summary data

    Returns
    -------
    polars.dataframe
        interval dataframe
    """
    intervals_modeled_obs = summary_df.filter(
        pl.col("processed_obs_data").is_null(),
        pl.col("reference_date") <= max(raw_processed_obs["reference_date"]),
    ).drop(["processed_obs_data", "expected_obs_cases", "expected_nowcast_cases"])
    return intervals_modeled_obs


def prepare_plot_data(date_to_use: date, bsc: BlobServiceClient):
    """
    Plotting funtion that combines all previous functions to read and generate gold_combined data as well as summary_pivot data and then generate final plotting datasets

    Parameters
    ----------
    blob_service_client: BlobServiceClient
        BlobServiceClient object
    date_to_use: date
        report date

    Returns
    -------
    polars.dataframes
        two datasets: one for observation data and one for interval
    """
    gold_combined = combine_gold_current_and_prev(date_to_use, blob_service_client=bsc)
    summary_pivot = read_and_process_summary_data(
        date_to_use=date_to_use, blob_service_client=bsc
    )

    obs_plot_data = process_obs_plot_data(
        merged_gold_dfs=gold_combined, summary_df=summary_pivot
    )
    interval_plot_data = process_interval_plot_data(
        raw_processed_obs=obs_plot_data, summary_df=summary_pivot
    )
    return obs_plot_data, interval_plot_data


if __name__ == "__main__":
    # Some sample inputs for testing. Need to move something like this to an actual test
    args = {"date_to_use": "2025-01-22"}
    obs_plot_data = prepare_plot_data(date_to_use=args)[0]
    interval_plot_data = prepare_plot_data(date_to_use=args)[1]
