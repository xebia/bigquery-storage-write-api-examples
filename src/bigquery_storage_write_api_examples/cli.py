import logging
import os
from enum import Enum
from typing import Annotated

import typer

from bigquery_storage_write_api_examples import __version__
from bigquery_storage_write_api_examples.examples.default_stream_writer_example import (
    DefaultStreamWriterExample,
)

logger = logging.getLogger("bigquery_storage_write_api_examples")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

os.environ["GRPC_VERBOSITY"] = "NONE"  # Disable grpc warning logs for BigQuery
# grpc library comes as part of google-cloud-bigquery-storage package
logging.getLogger("grpc").setLevel(logging.ERROR)


class Examples(Enum):
    DEFAULT_STREAM_WRITER = "default-stream-writer"
    PENDING_TYPE_STREAM_WRITER = "pending-stream-writer"
    COMMITTED_TYPE_STREAM_WRITER = "committed-stream-writer"
    BUFFERED_TYPE_STREAM_WRITER = "buffered-stream-writer"


app = typer.Typer(
    help="🖌 BigQuery Storage Write API Examples CLI",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
    pretty_exceptions_short=False,
)


@app.command(short_help="📌 Displays the current version number")
def version():
    print(__version__)


@app.command(
    name="dump-bigquery-table-schema",
    help="📋 Dump the BigQuery table schema",
    no_args_is_help=False,
)
def _dump_bigquery_table_schema(
    project_id: Annotated[str, typer.Argument(help="Project ID")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    table_id: Annotated[str, typer.Argument(help="Table ID")],
):
    pass


@app.command(
    name="run",
    help="👯 Run the BigQuery Storage Write API Example",
    no_args_is_help=False,
)
def _run(
    example: Annotated[Examples, typer.Argument(help="Example name")],
):
    logger.info(f"🥇 BigQuery Storage Write API Examples version={__version__}")
    logger.info(f"👯 Running example: {example}")
    match example:
        case Examples.DEFAULT_STREAM_WRITER:
            DefaultStreamWriterExample(project_id="", dataset_id="", table_id="").run()


def entrypoint():
    app()
