import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient
from tqdm import tqdm


def merge_task_files(
    min_runat: datetime,
    max_runat: datetime,
    prod_date: date | None,
    rt_output_container_name: str = "nssp_rt",
    post_process_container_name: str = "nssp_rt_post_process",
):
    """
    Merge multiple task sample and summary files within a specified time range.
    If a production date is provided, only files with the specified production date are
    included.

    Parameters
    ----------
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

    Returns
    -------
    None
        This function does not return any value.
    """
    # === Set up blob service clients ==================================================
    bsc: BlobServiceClient = instantiate_blob_service_client(
        DefaultAzureCredential(), AzureStorage.AZURE_STORAGE_ACCOUNT_URL
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
    metadata_files: list[str] = [
        b.name
        for b in input_ctr_client.list_blobs()
        if b.name.endswith("metadata.json") and (b.creation_time >= min_runat)
    ]
    # Download the metadata files into a tempdir
    with tempfile.TemporaryDirectory() as tempdir:
        for mf in tqdm(metadata_files, desc="Downloading metadata files"):
            with open(os.path.join(tempdir, mf), "wb") as f:
                f.write(input_ctr_client.download_blob(mf).readall())

        # === Using the metadata files, get the tasks we want to merge =================
        prod_runs: pl.DataFrame = (
            # FInd all the metadata files
            pl.scan_ndjson(os.path.join(tempdir, "**", "*.metadata.json"))
            # Filter by the run at times
            .filter(pl.col.run_at.is_between(min_runat, max_runat, closed="both"))
            .sort("run_at")
            # Keep just the most recently run tasks
            .unique(subset=["disease", "geo_value", "production_date"], keep="last")
            .collect()
        )

        # === Merge the sample files ===================================================
        # Download the samples files
        local_sample_files: list[str] = []
        for sf in tqdm(
            prod_runs.get_column("samples_path"), desc="Downloading samples"
        ):
            lsf = os.path.join(tempdir, sf)
            local_sample_files.append(lsf)
            with open(lsf, "wb") as f:
                f.write(input_ctr_client.download_blob(sf).readall())

        # Sort for nicer sorting in the final parquet
        local_sample_files.sort()
        # Merge the files
        final_samples = os.path.join(tempdir, "samples.parquet")
        pl.scan_parquet(local_sample_files).sort(["geo_value", "disease"]).sink_parquet(
            final_samples, statistics="full"
        )

        # === Merge the summary files ==================================================
        # Download the summary files
        local_summary_files: list[str] = []
        for sf in tqdm(
            prod_runs.get_column("summaries_paths"), desc="Downloading summaries"
        ):
            lsf = os.path.join(tempdir, sf)
            local_summary_files.append(lsf)
            with open(lsf, "wb") as f:
                f.write(input_ctr_client.download_blob(sf).readall())

        # Sort for nicer sorting in the final parquet
        local_summary_files.sort()
        # Merge the files
        final_summaries = os.path.join(tempdir, "summaries.parquet")
        pl.scan_parquet(local_summary_files).sort(
            ["geo_value", "disease"]
        ).sink_parquet(final_summaries, statistics="full")

        # === Upload the merged files to the post-process container ========================
        # Not yet sure if this is really what we want.
        blob_path_summary_file = os.path.join(
            os.path.commonprefix(local_summary_files), "summaries.parquet"
        )
        with open(final_summaries, "rb") as data:
            output_ctr_client.upload_blob(name=blob_path_summary_file, data=data)

        blob_path_samples_file = os.path.join(
            os.path.commonprefix(local_sample_files), "samples.parquet"
        )
        with open(final_samples, "rb") as data:
            output_ctr_client.upload_blob(name=blob_path_samples_file, data=data)


@dataclass
class AzureStorage:
    """
    For some useful constants
    """

    AZURE_STORAGE_ACCOUNT_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/"
    AZURE_CONTAINER_NAME: str = "rt-epinow2-config"
    SCOPE_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/.default"


def instantiate_blob_service_client(
    sp_credential: DefaultAzureCredential, account_url: str
) -> BlobServiceClient:
    """Instantiate Blob Service Client

    Function to instantiate blob service client to interact with Azure Blob Storage.

    Parameters
    ----------
    sp_credential : DefaultAzureCredential
        Service principal credential object for use in authenticating with Storage API.
    account_url : str
        URL of the storage account.

    Returns
    -------
    BlobServiceClient
        Azure Blob Storage client.

    Raises
    ------
    ValueError
        If sp_credential is invalid or BlobServiceClient fails to instantiate.
    """

    if not sp_credential:
        raise ValueError("Service principal credential not provided.")

    blob_service_client = BlobServiceClient(account_url, credential=sp_credential)

    return blob_service_client
