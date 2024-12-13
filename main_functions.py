from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
from azure.identity import EnvironmentCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient
from rich.console import Console
from rich.progress import track

console = Console()


def merge_task_files(
    release_name: str,
    min_runat: datetime,
    max_runat: datetime,
    prod_date: date | None,
    rt_output_container_name: str = "nssp_rt",
    post_process_container_name: str = "nssp-rt-post-process",
    overwrite_blobs: bool = False,
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
    |   |   ├── job_<jobid>/
    |   |   |   ├── merged.csv
    |   |   |   ├── plots/
    |   |   |   |   ├── choropleth.png
    |   |   |   |   ├── lineinterval.png
    |   |   |   |   ├── timeseries/
    |   |   |   |   |   ├── <location>.png
    |   |   |   |   |   ├── <location>.png
    |   |   |   |   |   ├── ...
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
        credential=EnvironmentCredential(),
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
    # Download the metadata files into the internal-review folder
    for mf in track(metadata_files, description="Downloading metadata files"):
        to_write = meta / mf
        to_write.parent.mkdir(parents=True, exist_ok=True)
        with open(to_write, "wb") as f:
            f.write(input_ctr_client.download_blob(mf).readall())

    # === Using the metadata files, get the tasks we want to merge =================
    md_path = str(meta / "**/metadata.json")
    # Use duckdb here bc polars apparen't can't read multiple json files unless they are
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
    )
    console.log(f"Found {len(prod_runs)} tasks to merge")

    # === Merge the sample files ===================================================
    # Download the samples files
    local_sample_files: list[Path] = []
    for sf in track(
        prod_runs.get_column("samples_path"),
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
    conn.sql(f"""
    CREATE VIEW samples AS FROM
    read_parquet([{lsf_str}]);

    COPY samples TO '{str(final_samples)}' (CODEC 'zstd');
    """)

    # === Merge the summary files ==================================================
    # Download the summary files
    local_summary_files: list[Path] = []
    for sf in track(
        prod_runs.get_column("summaries_path"),
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
    conn.sql(f"""
    CREATE VIEW summaries AS FROM
    read_parquet([{lsf_str}]);

    COPY summaries TO '{str(final_summaries)}' (CODEC 'zstd');
    """)

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
    try:
        md_file = meta / "metadata.parquet"
        prod_runs.write_parquet(md_file)
        with md_file.open("rb") as data:
            output_ctr_client.upload_blob(
                name=str(md_file),
                data=data,
                overwrite=overwrite_blobs,
            )
            console.log(f"Uploaded the metadata to {output_ctr_client.url}/{md_file}")
    except Exception as e:
        console.log(f"Failed to upload the metadata: {e}")

    # === Clean up =====================================================================
    conn.close()


@dataclass
class AzureStorage:
    """
    For some useful constants
    """

    AZURE_STORAGE_ACCOUNT_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/"
    AZURE_CONTAINER_NAME: str = "rt-epinow2-config"
    SCOPE_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/.default"


if __name__ == "__main__":
    # Note that the dates in blob storage are in UTC, so when looking for files, we need
    # to use UTC times.
    release_name = "2024-12-12"
    min_runat = datetime(2024, 12, 10, hour=21, minute=10, tzinfo=timezone.utc)
    max_runat = datetime(2024, 12, 10, hour=22, minute=20, tzinfo=timezone.utc)
    prod_date = date(2022, 1, 1)
    rt_output_container_name = "zs-test-pipeline-update"
    merge_task_files(
        release_name=release_name,
        min_runat=min_runat,
        max_runat=max_runat,
        prod_date=prod_date,
        rt_output_container_name=rt_output_container_name,
        overwrite_blobs=True,
    )
