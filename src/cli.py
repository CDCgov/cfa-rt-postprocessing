from datetime import datetime, timezone

import typer
from typing_extensions import Annotated

from cfa_rt_postprocessing.main_functions import merge_and_render_anomaly


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
    prod_date: Annotated[
        datetime | None,
        typer.Option(
            help="A production date to filter on. If not passed in, not filtered by production_date"
        ),
    ] = None,
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
    ] = True,
    is_prod_run: Annotated[
        bool,
        typer.Option(
            help=(
                "Whether this is a production run or not. If it is a production run, the "
                "production_index.csv file will be updated."
            ),
        ),
    ] = False,
):
    # Add UTC timezone to min_runat and max_runat. Typer cannot add timezone info to
    # parseddatetime objects
    min_runat = min_runat.replace(tzinfo=timezone.utc)
    max_runat = max_runat.replace(tzinfo=timezone.utc)

    # Typer apparently does not know how to parse a date, so we need to grab just the
    # date portion of the prod_date datetime object
    if prod_date is not None:
        prod_date = prod_date.date()  # type: ignore

    # If this is a prod_run, make sure that overwrite_blobs is set to True
    if is_prod_run and not overwrite_blobs:
        raise typer.BadParameter(
            "If this is a production run, the --overwrite-blobs flag must be set to True"
        )

    merge_and_render_anomaly(
        release_name=release_name,
        min_runat=min_runat,
        max_runat=max_runat,
        prod_date=prod_date,
        rt_output_container_name=rt_output_container_name,
        post_process_container_name=post_process_container_name,
        overwrite_blobs=overwrite_blobs,
        is_prod_run=is_prod_run,
    )


if __name__ == "__main__":
    typer.run(main)
