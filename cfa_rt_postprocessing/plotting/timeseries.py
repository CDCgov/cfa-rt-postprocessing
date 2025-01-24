import polars as pl
import polars as pl
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient
from io import BytesIO
from azure.storage.blob import BlobServiceClient


from dataclasses import dataclass
@dataclass
class AzureStorage:
    """
    For some useful constants
    """

    AZURE_STORAGE_ACCOUNT_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/"
    AZURE_CONTAINER_NAME: str = "rt-epinow2-config"
    SCOPE_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/.default"
    
    
pl.enable_string_cache() #ideally not use this, troubleshoot "StringCacheMismatchError"

####################
# Parameters for Test File
####################
bsc = BlobServiceClient(
    AzureStorage.AZURE_STORAGE_ACCOUNT_URL,
    credential=DefaultAzureCredential())
#production states function will be listed somwhere else 

# Manual run parameters --remove later
date_to_use = "2025-01-22"


# Reads files straight from blob (not read a local downloaded version)
def read_blob_file (container_name,
                    blob_file_path,
                    blob_service_client=bsc):
    input_container: ContainerClient = blob_service_client.get_container_client(container_name)
    blob_client = input_container.get_blob_client(blob_file_path)
    download_stream = blob_client.download_blob()
    content = download_stream.readall()  # Read all bytes of the blob
    bytes_io = BytesIO(content)
    df = pl.read_parquet(bytes_io)
    return df
    
def gold_data_formatting(gold_df,
                         value_name,
                         date_cut_off):
    nssp_gold  = (
        gold_df
        .filter(pl.col("metric")=="count_ed_visits")
        .with_columns([
            (pl.when(pl.col("disease") == "COVID-19/Omicron")
            .then(pl.lit("COVID-19"))
            .otherwise(pl.col("disease"))).cast(pl.String)
            .name.keep(),
            #pl.lit("obs_cases").alias("variable"),
            pl.col("geo_value").cast(pl.String)
            ])
        .filter( (pl.col("disease")=="COVID-19") | (pl.col("disease")=="Influenza") , # Filter based on pathogen_to_run
                pl.col("reference_date") >= date_cut_off)  
        )
    # Summarize health data by reference_date, geo_value, and disease
    healthdata_df_agg = (
        nssp_gold
        .group_by(["reference_date", "geo_value", "disease"])
        .agg(pl.col("value").sum().alias("value"))
        .rename({
            "value": value_name
        })
        )
    return healthdata_df_agg

def combine_gold_current_and_prev(date_to_use,
                                  blob_service_client=bsc):
    end_date = datetime.strptime(date_to_use, "%Y-%m-%d")
    past_week =end_date - timedelta(days=7)
    start_date = end_date - timedelta(weeks=24)###
    date_cut_off = end_date - timedelta(weeks=8)
    # read the gold files (same date as summaries.parquet and week prior)
    nssp_gold = read_blob_file(blob_service_client=blob_service_client,
                                container_name="nssp-etl",
                                blob_file_path="gold/"+end_date.strftime("%Y-%m-%d") +".parquet")
    nssp_gold_previous_wk = read_blob_file(blob_service_client=blob_service_client,
                                container_name="nssp-etl",
                                blob_file_path="gold/"+past_week.strftime("%Y-%m-%d") +".parquet")

    
    healthdata_df_agg = gold_data_formatting(gold_df=nssp_gold,
                                             value_name="raw_obs_data",
                                             date_cut_off=date_cut_off)
    
    healthdata_df_agg_prev_week = gold_data_formatting(gold_df=nssp_gold_previous_wk,
                                             value_name="raw_obs_data_prev_wk",
                                             date_cut_off=date_cut_off)
    
    healthdata_df_agg_join = (
        healthdata_df_agg
        .select(["reference_date","geo_value","disease", "raw_obs_data"])
        .join(healthdata_df_agg_prev_week.select(["reference_date","geo_value","disease", "raw_obs_data_prev_wk"]),
                        on=["reference_date", "geo_value", "disease"],
                        how="left")
    )
    # Create US data by summarizing across all states and then adding 'US' as a state
    us_df_agg = (
        healthdata_df_agg_join
        .filter(pl.col("geo_value") != "US")  # Exclude existing 'US' rows if any
        .group_by(["reference_date", "geo_value", "disease"])
        .agg(
            pl.col("raw_obs_data").sum().alias("raw_obs_data"),
            pl.col("raw_obs_data_prev_wk").sum().alias("raw_obs_data_prev_wk")
            )
        .with_columns(pl.lit("US").alias("geo_value"))
        #.select(["reference_date","geo_value","disease","value"])
    )
    # Combine health data with US data
    healthdata_df_combined = pl.concat([healthdata_df_agg_join, us_df_agg])
    return healthdata_df_combined

def read_and_process_summary_data (date_to_use,
                                   blob_service_client=bsc,
                                   variable_values=['processed_obs_data','expected_nowcast_cases', 'expected_obs_cases']):
    end_date = datetime.strptime(date_to_use, "%Y-%m-%d")
    # Read the summaries parquet file
    summary_df = read_blob_file(blob_service_client=blob_service_client,
                                container_name="nssp-rt-post-process",
                                blob_file_path=end_date.strftime("%Y-%m-%d")+"/internal-review/summaries.parquet")
    summary_pivot = (summary_df
                    .filter(pl.col("_variable").is_in(variable_values))
                    .pivot(on="_variable",
                        values=["value", "_lower", "_upper"],
                        index=["time", "reference_date","geo_value","disease", "_width"])
                    .drop(["_upper_processed_obs_data", "_lower_processed_obs_data"])
                    .rename({
                            "value_processed_obs_data": "processed_obs_data",
                            "value_expected_obs_cases": "expected_obs_cases",
                            "value_expected_nowcast_cases": "expected_nowcast_cases",
                        })
                    .sort(['disease', 
                    'geo_value', 
                    'reference_date'])
    )
    return summary_pivot


def process_obs_plot_data (merged_gold_dfs,
                           summary_df):
    raw_processed_obs = (
        merged_gold_dfs
        .join(
            (
                summary_df
                .group_by(['time',
                        'reference_date',
                        "geo_value",
                        "disease"])
                .agg([pl.max("processed_obs_data"),
                    pl.max("expected_obs_cases"),
                    pl.max("expected_nowcast_cases")]
                    )
                ),
            on=["reference_date", "geo_value", "disease"],how="left"
            )
        )
    return raw_processed_obs

def process_interval_plot_data (raw_processed_obs,
                                summary_df):
    intervals_modeled_obs = (
        (
        summary_df
        .filter(pl.col('processed_obs_data').is_null(),
                pl.col('reference_date') <= max(raw_processed_obs['reference_date'])) 
        .drop(["processed_obs_data","expected_obs_cases","expected_nowcast_cases"])
        )
        )
    return intervals_modeled_obs


def prepare_plot_data(date_to_use):
    gold_combined = combine_gold_current_and_prev(date_to_use,
                                                  blob_service_client=bsc)
    summary_pivot = read_and_process_summary_data(date_to_use=date_to_use)
    
    obs_plot_data=process_obs_plot_data(merged_gold_dfs=gold_combined,
                                        summary_df=summary_pivot)
    interval_plot_data =process_interval_plot_data(raw_processed_obs=obs_plot_data,
                                                   summary_df=summary_pivot)
    return obs_plot_data, interval_plot_data
    

obs_plot_data = prepare_plot_data(date_to_use=date_to_use)[0]
interval_plot_data= prepare_plot_data(date_to_use=date_to_use)[1]


###############################################
# Plotting
###############################################

def timeseries_plot (date_to_use,
                     state,
                     pathogen,
                     obs_plot_data,
                     interval_plot_data):

    import plotly.graph_objs as go
    from plotly.subplots import make_subplots
    import plotly.express as px #requires pandas

    end_date = datetime.strptime(date_to_use, "%Y-%m-%d")
    df_obs = (
        obs_plot_data
        .filter(pl.col("geo_value")==state,
                pl.col('disease')==pathogen)
        .select(["reference_date", "geo_value", "disease",  "processed_obs_data", "raw_obs_data","raw_obs_data_prev_wk","expected_obs_cases", "expected_nowcast_cases"])
        .sort(['reference_date'])
    )
    df_interval = (
        interval_plot_data
        .filter(pl.col("geo_value")==state,
                pl.col('disease')==pathogen)
        .sort(['reference_date'])
    )

    # Line Traces
    est_current_reported_trace = go.Scatter(x=df_obs['reference_date'], y=df_obs['expected_obs_cases'], mode='lines', name='Est. Current Reported', line_color="black")
    est_total_reported_trace = go.Scatter(x=df_obs['reference_date'], y=df_obs['expected_nowcast_cases'], mode='lines', name='Est. Total Reported', line=dict(dash='dash'), line_color="black")
    
    # Marker Traces
    processed_reported_trace = go.Scatter(x=df_obs['reference_date'], y=df_obs['processed_obs_data'], mode='markers', name='Processed Reported', marker=dict(symbol='circle'), visible='legendonly')
    raw_reported_trace = go.Scatter(x=df_obs['reference_date'], y=df_obs['raw_obs_data'], mode='markers', name='Raw Reported', marker=dict(symbol='circle', color="black"))
    raw_prev_wk_reported_trace = go.Scatter(x=df_obs['reference_date'], y=df_obs['raw_obs_data_prev_wk'], mode='markers', name='Raw Reported (Prev Week)', marker=dict(symbol='cross', color='rgba(255, 0, 0, 0.8)', size =5))

    # Combine all traces into a list
    traces = [est_current_reported_trace, est_total_reported_trace, processed_reported_trace, raw_reported_trace, raw_prev_wk_reported_trace]

    # Create a figure with the collected traces
    fig = go.Figure(data=traces)

    # Loop through each width group to add ribbon traces
    for group in df_interval['_width'].unique():
        group_data = df_interval.filter(pl.col('_width') == group)
        # Add trace for the ribbon area for each group
        fig.add_trace(go.Scatter(
            x=group_data['reference_date'].to_list() + group_data['reference_date'].to_list()[::-1],
            y=group_data['_upper_expected_nowcast_cases'].to_list() + group_data['_lower_expected_nowcast_cases'].to_list()[::-1],
            fill='toself',
            fillcolor='rgba(75, 63, 63, 0.1)',
            line=dict(color='rgba(151, 127, 105, 0.05)'),
            name='Est. Total Interval: '+str(group)
        ))
    # Define background color according to pathogen
    if pathogen=="COVID-19":
        fig.update_layout(
            #paper_bgcolor='rgba(255, 255, 255, 0.2)',  # Sets the color of the paper (area outside the plot)
            plot_bgcolor='rgb(235, 229, 229)',       # Sets the color of the plot area
            legend=dict(bgcolor='rgba(226, 225, 225, 0.6)',
                bordercolor="Black",
                borderwidth=1)
        )
    else:
        fig.update_layout(
            #paper_bgcolor='rgba(255, 255, 255, 0.2)',  # Sets the color of the paper (area outside the plot)
            plot_bgcolor='rgba(107, 174, 214, 0.2)',       # Sets the color of the plot area
            legend=dict(bgcolor='rgba(107, 174, 214, 0.1)',
                bordercolor="Black",
                borderwidth=1)
        )

    # Update the title
    fig.update_layout(title=state + ": "+ pathogen + " ED Visits")
    # Update the x-axis title
    fig.update_xaxes(title_text="Reference Date")
    # Update the y-axis title
    fig.update_yaxes(title_text="Incident ED Visits")

    # Save the figure as an HTML file
    fig.write_html(state + "_"+ pathogen + "_" + end_date.strftime("%Y-%m-%d") + ".html")
    # Show the plot
    fig.show()

timeseries_plot (date_to_use=date_to_use,
                     state="CA",
                     pathogen="COVID-19",
                     obs_plot_data=obs_plot_data,
                     interval_plot_data=interval_plot_data)


