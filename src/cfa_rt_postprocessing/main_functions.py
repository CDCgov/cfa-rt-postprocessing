import warnings
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from shutil import rmtree

import duckdb
import polars as pl
import quarto
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
    ContentSettings,
)
from rich.console import Console
from rich.progress import track

from src.cfa_rt_postprocessing.azure_constants import AzureStorage

console = Console()


def validate_args(args):
    """
    Validate the arguments passed to the merge function.

    Parameters
    ----------
    args : dict
        The arguments passed to the merge function.

    Returns
    -------
    dict
        The validated arguments.
    """
    if "release_name" not in args or "min_runat" not in args or "max_runat" not in args:
        raise ValueError("release_name, min_runat, and max_runat are required")

    # Validate the datetime arguments
    try:
        min_runat = datetime.fromisoformat(args.get("min_runat")).replace(
            tzinfo=timezone.utc
        )
        max_runat = datetime.fromisoformat(args.get("max_runat")).replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise ValueError("min_runat and max_runat must be ISO-formatted strings")

    # Validate the optional arguments
    rt_output_container_name = args.get("rt_output_container_name", "nssp_rt")
    post_process_container_name = args.get(
        "post_process_container_name", "nssp-rt-post-process"
    )
    overwrite_blobs = args.get("overwrite_blobs", False)

    # If this is a prod run, make sure overwite_blobs is True, otherwise error out
    is_prod_run = args.get("is_prod_run", False)
    if (overwrite_blobs is False) and is_prod_run:
        raise ValueError("overwrite_blobs must be True for a production run")

    return {
        "release_name": args.get("release_name"),
        "min_runat": min_runat,
        "max_runat": max_runat,
        "rt_output_container_name": rt_output_container_name,
        "post_process_container_name": post_process_container_name,
        "overwrite_blobs": overwrite_blobs,
        "is_prod_run": is_prod_run,
    }


def merge_and_render_anomaly(
    release_name: str,
    min_runat: datetime,
    max_runat: datetime,
    prod_date: date | None = None,
    rt_output_container_name: str = "nssp_rt",
    post_process_container_name: str = "nssp-rt-post-process",
    overwrite_blobs: bool = False,
    is_prod_run: bool = False,
):
    """
    Merge multiple task sample and summary files within a specified time range.
    If a production date is provided, only files with the specified production date are
    included.

    Sets up the desired folder structure
    ├── <release_name>/          # E.g. "20241009"
    │   ├── internal-review/
    |   |   ├── job_<jobid>/     # Jobs are disease-specific in production
    |   |   |   ├── merged.csv
    |   |   |   ├── plots/
    |   |   |   |   ├── choropleth.png
    |   |   |   |   ├── lineinterval.png
    |   |   |   |   ├── timeseries/
    |   |   |   |   |   ├── <location>.png
    |   |   |   |   |   ├── <location>.png
    |   |   |   |   ├── ...
    |   ├── release/
    |   |   ├── merged_release.csv
    |   |   ├── <rundate>_<disease1>_map_data.csv
    |   |   ├── <rundate>_<disease2>_map_data.csv
    |   |   ├── <rundate>_<disease1>_timeseries_data.csv
    |   |   ├── <rundate>_<disease2>_timeseries_data.csv

    Parameters
    ----------
    release_name : str
        The name of the release. E.g. "2024-12-12"
    min_runat : datetime
        The minimum run_at time to include.
    max_runat : datetime
        The maximum run_at time to include.
    prod_date : date, optional
        The production date to filter by. If None, no production date filtering is applied.
    rt_output_container_name : str, optional
        The name of the Rt output container. Default is "nssp_rt".
    post_process_container_name : str, optional
        The name of the post-process container. Default is "nssp_rt_post_process".
    overwrite_blobs : bool, optional
        If True, overwrite the blobs in the post-process container. Default is
        False.

    Returns
    -------
    None
        This function does not return any value.
    """
    # === Set up the desired folder structure ==========================================
    # This function will run inside an Azure Function, and be given a fresh file system
    # each time it runs.
    console.status("Setting up the desired folder structure")
    root = Path(".") / release_name
    internal_review = root / "internal-review"
    meta = internal_review / "meta"
    release = root / "release"
    for d in [root, internal_review, release, meta]:
        d.mkdir(parents=True, exist_ok=True)

    # === Set up blob service clients ==================================================
    console.status("Setting up blob service clients")
    bsc = BlobServiceClient(
        AzureStorage.AZURE_STORAGE_ACCOUNT_URL,
        credential=DefaultAzureCredential(),
    )
    input_ctr_client: ContainerClient = bsc.get_container_client(
        rt_output_container_name
    )
    output_ctr_client: ContainerClient = bsc.get_container_client(
        post_process_container_name
    )

    # === Find and download all of the metadata files ==================================
    # Get a list of the metadata files, just the ones that were created at or after
    # `min_runat`. Can revisit this datetime cutoff if needed
    console.status("Finding metadata files")
    metadata_files: list[str] = [
        b.name
        for b in input_ctr_client.list_blobs()
        if b.name.endswith("metadata.json")
        and (b.creation_time >= min_runat)
        and (b.creation_time <= max_runat)
    ]
    console.status(f"Found {len(metadata_files)} metadata files")
    # Download the metadata files into the internal-review folder
    for mf in track(metadata_files, description="Downloading metadata files"):
        to_write = meta / mf
        to_write.parent.mkdir(parents=True, exist_ok=True)
        with open(to_write, "wb") as f:
            f.write(input_ctr_client.download_blob(mf).readall())

    # === Using the metadata files, get the tasks we want to merge =================
    md_path = str(meta / "**/metadata.json")

    # Use duckdb here bc polars apparently can't read multiple json files unless they are
    # ndjson, and these are not
    conn = duckdb.connect()

    prod_runs: pl.DataFrame = (
        # Find all the metadata files
        conn.sql(f"SELECT * FROM read_json('{md_path}', auto_detect=true)")
        .pl()
        # Make run_at a datetime. Do it in polars bc doing it in duckdb loses the tzinfo
        # when transferring to polars
        .with_columns(pl.col.run_at.str.to_datetime("%Y-%m-%dT%H:%M:%S%z"))
        # Filter by the run at times
        .filter(pl.col.run_at.is_between(min_runat, max_runat, closed="both"))
        .sort("run_at")
        # Keep just the most recently run tasks
        .unique(subset=["disease", "geo_value", "production_date"], keep="last")
        # Add paths to the samples and summaries from inside the blob container.
        # These are not necessarily the same as the sample and summary paths in the
        # metadata files, so we need to add them here so we know where to look in
        # the blob container.
        .with_columns(
            blob_samples_path=pl.col.job_id + "/samples/" + pl.col.task_id + ".parquet",
            blob_summaries_path=pl.col.job_id
            + "/summaries/"
            + pl.col.task_id
            + ".parquet",
        )
    )
    # Filter by the production date if provided
    if prod_date is not None:
        prod_runs = prod_runs.filter(pl.col.production_date.eq(prod_date))

    console.log(f"Found {len(prod_runs)} tasks to merge")
    # === Create the <release-name>/interal_review/<job_id>/ folders ===============
    # Get the unique job-ids
    job_ids: list[str] = prod_runs.get_column("job_id").unique().to_list()

    # Create the job folders
    for job_id in job_ids:
        job_folder = internal_review / job_id
        job_folder.mkdir(parents=True, exist_ok=True)

    # === Merge the sample files ===================================================
    # Download the samples files
    local_sample_files: list[Path] = []
    for sf in track(
        prod_runs.get_column("blob_samples_path"),
        description="Downloading samples",
    ):
        lsf: Path = internal_review / sf
        lsf.parent.mkdir(parents=True, exist_ok=True)
        local_sample_files.append(lsf)
        with lsf.open("wb") as f:
            f.write(input_ctr_client.download_blob(sf).readall())

    # Sort for nicer sorting in the final parquet
    local_sample_files.sort()

    # Create a string of the file names readable by duckdb
    lsf_str = ",".join("'" + str(p) + "'" for p in local_sample_files)

    # Merge the files
    final_samples = internal_review / "samples.parquet"
    console.log("Merging the sample files")
    console.log(local_sample_files)

    # Merge the files with duckdb for better RAM usage
    conn.sql(
        f"""
    CREATE VIEW samples AS FROM
    read_parquet([{lsf_str}]);

    COPY samples TO '{str(final_samples)}'
    -- compression level only works with zstd. Compress a lot so we can fit on
    -- the Azure Function node disk space. min 1, max 22
    (CODEC 'zstd', COMPRESSION_LEVEL 15);
    """
    )

    # === Merge the summary files ==================================================
    # Download the summary files
    local_summary_files: list[Path] = []
    for sf in track(
        prod_runs.get_column("blob_summaries_path"),
        description="Downloading summaries",
    ):
        lsf: Path = internal_review / sf
        lsf.parent.mkdir(parents=True, exist_ok=True)
        local_summary_files.append(lsf)
        with lsf.open("wb") as f:
            f.write(input_ctr_client.download_blob(sf).readall())

    # Sort for nicer sorting in the final parquet
    local_summary_files.sort()

    # Create a string of the file names readable by duckdb
    lsf_str = ",".join("'" + str(p) + "'" for p in local_summary_files)

    # Merge the files
    final_summaries = internal_review / "summaries.parquet"
    console.log("Merging the summary files")
    console.log(local_summary_files)

    # Merge the files with duckdb for better RAM usage
    conn.sql(
        f"""
    CREATE VIEW summaries AS FROM
    read_parquet([{lsf_str}]);

    COPY summaries TO '{str(final_summaries)}'
    -- compression level only works with zstd. Compress a lot so we can fit on
    -- the Azure Function node disk space. min 1, max 22
    (CODEC 'zstd', COMPRESSION_LEVEL 15);
    """
    )

    # === Upload the merged files to the post-process container ========================
    console.status("Uploading the merged files to the post-process container")
    try:
        with final_summaries.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(final_summaries),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(
                f"Uploaded the summaries to {output_ctr_client.url}/{final_summaries}"
            )
    except Exception as e:
        console.log(f"Failed to upload the summaries: {e}")

    try:
        with final_samples.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(final_samples),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(
                f"Uploaded the samples to {output_ctr_client.url}/{final_samples}"
            )
    except Exception as e:
        console.log(f"Failed to upload the samples: {e}")

    # Upload the metadata df as a parquet file
    md_file = internal_review / "metadata.parquet"
    prod_runs.write_parquet(md_file)
    try:
        with md_file.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(md_file),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(f"Uploaded the metadata to {output_ctr_client.url}/{md_file}")
    except Exception as e:
        console.log(f"Failed to upload the metadata: {e}")

    # === Render and upload anomaly report =============================================
    console.status("Rendering the anomaly reports")
    covid_report = internal_review / "covid_anomaly_report.html"
    flu_report = internal_review / "influenza_anomaly_report.html"
    try:
        # COVID-19
        render_report(
            disease="COVID-19",
            desired_output_location=covid_report,
            summary_loc=final_summaries,
            samples_loc=final_samples,
            metadata_file=md_file,
        )
    except Exception as e:
        console.log(f"Failed to render the COVID-19 anomaly report: {e}")

    try:
        # Influenza
        render_report(
            disease="Influenza",
            desired_output_location=flu_report,
            summary_loc=final_summaries,
            samples_loc=final_samples,
            metadata_file=md_file,
        )
    except Exception as e:
        console.log(f"Failed to render the Influenza anomaly report: {e}")

    # Upload the reports
    try:
        if covid_report.exists():
            # To the usual location in the folder for this run
            with covid_report.open("rb") as data:
                output_ctr_client.upload_blob(
                    name=str(covid_report),
                    data=data,
                    overwrite=overwrite_blobs,
                )
            console.log(
                (
                    f"Uploaded the covid anomaly report to {output_ctr_client.url}/{covid_report}"
                    f" and to {output_ctr_client.url}/latest_anomaly_report_covid.html"
                )
            )

            # To /latest_anomaly_report_covid.html in the container
            with covid_report.open("rb") as data:
                output_ctr_client.upload_blob(
                    name="latest_anomaly_report_covid.html",
                    data=data,
                    overwrite=overwrite_blobs,
                )
        else:
            console.log("No covid report to upload. Skipping.")
    except Exception as e:
        console.log(f"Failed to upload the anomaly report: {e}")

    try:
        if flu_report.exists():
            # To the usual location in the folder for this run
            with flu_report.open("rb") as data:
                output_ctr_client.upload_blob(
                    name=str(flu_report),
                    data=data,
                    overwrite=overwrite_blobs,
                )

            # To /latest_anomaly_report_flu.html in the container
            with flu_report.open("rb") as data:
                output_ctr_client.upload_blob(
                    name="latest_anomaly_report_flu.html",
                    data=data,
                    overwrite=overwrite_blobs,
                )

            console.log(
                (
                    f"Uploaded the flu anomaly report to {output_ctr_client.url}/{flu_report}"
                    f" and to {output_ctr_client.url}/latest_anomaly_report_flu.html"
                )
            )
        else:
            console.log("No flu report to upload. Skipping.")
    except Exception as e:
        console.log(f"Failed to upload the flu anomaly report: {e}")

    # === Calculate the categories for the samples =====================================
    console.status("Calculating the categories from the samples")
    p_growing = calculate_categories(final_samples)

    # Save it to file as parquet, and as CSV
    p_growing_pq_file = internal_review / "p_growing.parquet"
    p_growing_csv_file = internal_review / "p_growing.csv"
    p_growing.write_parquet(p_growing_pq_file)
    p_growing.write_csv(p_growing_csv_file)

    # Upload the files
    try:
        with p_growing_pq_file.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(p_growing_pq_file),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(
                f"Uploaded the p_growing parquet to {output_ctr_client.url}/{p_growing_pq_file}"
            )

        with p_growing_csv_file.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(p_growing_csv_file),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(
                f"Uploaded the p_growing csv to {output_ctr_client.url}/{p_growing_csv_file}"
            )
    except Exception as e:
        console.log(f"Failed to upload the p_growing files: {e}")

    # === Update the production index ==================================================
    if is_prod_run:
        # Get the production index blobs
        prod_idx_blobs: list[BlobProperties] = [
            b
            for b in output_ctr_client.list_blobs()
            if b.name.startswith("production_index") and b.name.endswith(".csv")
        ]
        console.log(f"Found {len(prod_idx_blobs)} production index files")
        # Find the most recent one
        most_recent_prod_idx: BlobProperties = max(
            prod_idx_blobs, key=lambda x: x.creation_time
        )
        console.log(f"Using most recent production index: {most_recent_prod_idx.name}")

        # Download the production index
        csv_bytes_io = BytesIO(
            output_ctr_client.download_blob(most_recent_prod_idx.name).readall()
        )
        production_index = pl.read_csv(
            csv_bytes_io,
            schema=pl.Schema(
                [
                    ("release_date", pl.Date),
                    ("run_date", pl.Date),
                ]
            ),
        ).lazy()

        # Get the release date
        release_date: date = round_up_to_friday(date.today())

        # Get this from the metadata. There should only be one unique run_at date
        # from all of these tasks
        run_dates: list[date] = (
            prod_runs.get_column("run_at").dt.date().unique().to_list()
        )
        if len(run_dates) != 1:
            # Warn the user that there was more than one run date. Note that in the
            # metadata from the model, `production_date` is what we are calling `run_date`
            # in the production index.
            warnings.warn(
                f"More than one unique run date found: {run_dates}. Using the first one."
            )

        run_date: date = run_dates[0]

        # Update it
        new_production_index: pl.DataFrame = update_production_index(
            production_index=production_index,
            release_date=release_date,
            run_date=run_date,
        ).collect()

        # Write it to CSV, using timestamp up to the current second as the name
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        new_prod_idx_file = internal_review / "production_index" / f"{now}.csv"
        new_prod_idx_file.parent.mkdir(parents=True, exist_ok=True)
        new_production_index.write_csv(new_prod_idx_file)

        # Upload it
        try:
            with new_prod_idx_file.open("rb") as data:
                output_ctr_client.upload_blob(
                    name=f"production_index/{now}.csv",
                    data=data,
                    overwrite=False,
                    # Small QOL improvement: set the content type to CSV so that various
                    # services know how to handle it
                    content_settings=ContentSettings(content_type="text/csv"),
                )
            console.log(
                f"Uploaded the production index to {output_ctr_client.url}/production_index/{now}.csv"
            )
        except Exception as e:
            console.log(f"Failed to upload the production index: {e}")

    # === Clean up =====================================================================
    conn.close()
    console.log(f"Cleaning up {root} folder")
    rmtree(root)


def render_report(
    disease: str,
    desired_output_location: Path,
    summary_loc: Path,
    samples_loc: Path,
    metadata_file: Path,
    unrendered_location: Path = Path("src/cfa_rt_postprocessing/anomaly_report.qmd"),
):
    quarto.render(
        input=unrendered_location,
        execute_params={
            "summary_file": str(summary_loc.absolute()),
            "samples_file": str(samples_loc.absolute()),
            "metadata_file": str(metadata_file.absolute()),
            "disease": disease,
        },
    )

    # Move the rendered report to the internal-review folder
    rendered_report = unrendered_location.parent / (unrendered_location.stem + ".html")
    rendered_report.replace(desired_output_location)
    console.status(
        f"Moved the rendered {disease} report to {str(desired_output_location)}"
    )


def calculate_categories(samples_file: Path) -> pl.DataFrame:
    """
    Takes in the path to the samples parquet file, calculates the five categories for
    each geo_value, disease, and reference_date.

    Returns a DataFrame with the columns:
    - geo_value
    - disease
    - reference_date
    - p_growing
    - five_cat_p_growing

    The samples file is fairly large, so use duckdb to help things go faster and more
    efficiently.
    """
    conn = duckdb.connect()
    conn.sql(
        f"""
    -- First create a 'view' of the samples. Don't use a table, because that would read
    -- it all into RAM. ATM, a single samples file is about 1.7GB, but attempting to load
    -- it into RAM crashes my 32GB machine.
    CREATE OR REPLACE VIEW samples AS
    SELECT reference_date, geo_value, disease, "value" as Rt
    FROM '{str(samples_file.absolute())}'
    WHERE "_variable" = 'Rt';

    -- Calculate the p_growing for each geo_value, disease, and reference_date
    CREATE OR REPLACE TABLE p_growing AS SELECT
    geo_value, disease, reference_date,
    AVG(IF(Rt > 1, 1, 0)) AS p_growing,
    CASE
        WHEN (p_growing > 0.9) AND (p_growing <= 1.0) THEN 'Growing'
        WHEN (p_growing > 0.75) AND (p_growing <= 0.9) THEN 'Likely Growing'
        WHEN (p_growing > 0.25) AND (p_growing <= 0.75) THEN 'Not Changing'
        WHEN (p_growing > 0.10) AND (p_growing <= 0.25) THEN 'Likely Declining'
        WHEN (p_growing >= 0.0) AND (p_growing <= 0.10) THEN 'Declining'
    END AS five_cat_p_growing
    FROM samples
    GROUP BY ALL
    ORDER BY ALL;
    """
    )

    p_growing = conn.sql("SELECT * FROM p_growing").pl()
    conn.close()

    return p_growing


def update_production_index(
    production_index: pl.LazyFrame, release_date: date, run_date: date
) -> pl.LazyFrame:
    """
    Update or add a row in the production index.

    Parameters
    ----------
    production_index : pl.LazyFrame
        The current production index LazyFrame containing release_date and
        run_date columns
    release_date : date
        The date on which this will be released to the website.
    run_date : date
        The date on which the model run happened.

    Returns
    -------
    pl.LazyFrame
        Updated production index with either modified run date for existing
        release date or new row added

    Notes
    -----
    Taking in the current state of the production index, update it with a run.
    If the release_date is already in the index, update the run_date for that
    row. If not, add a new row.
    """
    # Create the new row
    new_row = pl.LazyFrame(dict(release_date=[release_date], run_date=[run_date]))

    # Perform the update. Using `how="full"`, if this production week is already in the
    # index then it will be update "in place", otherwise a new row will be added.
    # Sort by release_date to ensure that the output is in the same order as the
    # input. Local testing has shown that sorting is sometimes necessary to keep the
    # order correct.
    return production_index.update(other=new_row, on="release_date", how="full").sort(
        "release_date"
    )


def round_up_to_friday(d: date) -> date:
    """
    Round a date up to the next Friday.

    Parameters
    ----------
    d : date
        The date to round up to the next Friday.

    Returns
    -------
    date
        The next Friday after the input date.
    """

    return d + timedelta(days=(4 - d.weekday()) % 7)


if __name__ == "__main__":
    # Some sample inputs for testing. Need to move something like this to an actual test
    args = {
        "release_name": "2025-01-14",
        "min_runat": "2024-12-17T19:40:06",
        "max_runat": "2024-12-18T00:00:00",
        "rt_output_container_name": "zs-test-pipeline-update",
        "post_process_container_name": "nssp-rt-post-process",
        "overwrite_blobs": True,
    }
    validated_args = validate_args(args)
    merge_and_render_anomaly(**validated_args)
