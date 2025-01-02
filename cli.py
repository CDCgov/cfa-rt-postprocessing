from datetime import datetime, timezone

import typer
from typing_extensions import Annotated

from main_functions import merge_task_files


def main(
    release_name: Annotated[
        str,
        typer.Argument(
            help="Name of the release. Usually the date of publication, e.g. 2025-01-01",
        ),
    ],
    min_runat: Annotated[
        datetime,
        typer.Argument(
            help="Minimum run datetime to consider for merging. UTC tz added by script."
        ),
    ],
    max_runat: Annotated[
        datetime,
        typer.Argument(
            help="Maximum run UTC datetime to consider for merging. UTC tz added by script."
        ),
    ],
    rt_output_container_name: Annotated[
        str,
        typer.Option(
            help="Name of the container where the RT output files are stored",
        ),
    ] = "nssp-rt",
    post_process_container_name: Annotated[
        str,
        typer.Option(
            help="Name of the container where the post process files are stored",
        ),
    ] = "nssp-rt-post-process",
    overwrite_blobs: Annotated[
        bool,
        typer.Option(
            help="Whether to overwrite the blobs in the post process container",
        ),
    ] = False,
):
    # Add UTC timezone to min_runat and max_runat. Typer cannot add timezone info to
    # parseddatetime objects
    min_runat = min_runat.replace(tzinfo=timezone.utc)
    max_runat = max_runat.replace(tzinfo=timezone.utc)
    merge_task_files(
        release_name=release_name,
        min_runat=min_runat,
        max_runat=max_runat,
        rt_output_container_name=rt_output_container_name,
        post_process_container_name=post_process_container_name,
        overwrite_blobs=overwrite_blobs,
    )


if __name__ == "__main__":
    typer.run(main)
