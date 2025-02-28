import logging
import os
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer
import yaml

from bigquery_storage_write_api_examples import Config, __version__
from bigquery_storage_write_api_examples.examples.default_stream_writer_example import (
    DefaultStreamWriterExample,
)
from bigquery_storage_write_api_examples.prepare_bigquery import PrepareBigQueryService

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


@app.command(
    name="bq-init",
    help="📊 Create dataset and tables in BigQuery if needed",
    no_args_is_help=False,
)
def bigquery_init(
    path_to_config: Annotated[str, typer.Option(help="Path to config file")] = "conf.yaml",
):
    _path_to_config = Path(path_to_config).resolve()
    if not _path_to_config.exists():
        raise FileNotFoundError(
            f"Config file at '{path_to_config}' does not exist, please copy conf.yaml.example to conf.yaml and fill in the values"
        )

    with _path_to_config.open("r") as inFile:
        cnf_raw = yaml.safe_load(inFile)
        config_ = Config(**cnf_raw)
        logger.info("✅ Config validation passed!")

        PrepareBigQueryService(config_).prepare()
        logger.info("✅ BigQuery infrastructure prepared!")


def entrypoint():
    app()
