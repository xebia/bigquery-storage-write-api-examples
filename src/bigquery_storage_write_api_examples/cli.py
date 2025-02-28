import json
import logging
import os
import warnings
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer
import yaml

from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.examples.committed_type_stream_writer_example import (
    CommittedTypeStreamWriterExample,
)
from bigquery_storage_write_api_examples.examples.default_stream_writer_example import (
    DefaultStreamWriterExample,
)
from bigquery_storage_write_api_examples.examples.pending_type_stream_writer_example import (
    PendingTypeStreamWriterExample,
)
from bigquery_storage_write_api_examples.prepare_bigquery import PrepareBigQueryService
from bigquery_storage_write_api_examples.proto_file import ProtoFileGenerator

logger = logging.getLogger("bigquery_storage_write_api_examples")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

os.environ["GRPC_VERBOSITY"] = "NONE"  # Disable grpc warning logs for BigQuery
# grpc library comes as part of google-cloud-bigquery-storage package
logging.getLogger("grpc").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


class Examples(Enum):
    DEFAULT_STREAM_WRITER = "default-stream-writer"
    PENDING_TYPE_STREAM_WRITER = "pending-type-stream-writer"
    COMMITTED_TYPE_STREAM_WRITER = "committed-type-stream-writer"
    BUFFERED_TYPE_STREAM_WRITER = "buffered-type-stream-writer"


app = typer.Typer(
    help="🖌 BigQuery Storage Write API Examples CLI",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
    pretty_exceptions_enable=False,
    pretty_exceptions_short=False,
)


@app.command(
    name="run",
    help="👯 Run the BigQuery Storage Write API Example",
    no_args_is_help=False,
)
def _run(
    example: Annotated[Examples, typer.Argument(help="Example name")],
    path_to_config: Annotated[str, typer.Option(help="Path to config file")] = "conf.yaml",
):
    logger.info(f"👯 Running example: {example}")
    config_ = _load_config(path_to_config)
    match example:
        case Examples.DEFAULT_STREAM_WRITER:
            DefaultStreamWriterExample(config_).run()
        case Examples.PENDING_TYPE_STREAM_WRITER:
            PendingTypeStreamWriterExample(config_).run()
        case Examples.COMMITTED_TYPE_STREAM_WRITER:
            CommittedTypeStreamWriterExample(config_).run()
        # case Examples.BUFFERED_TYPE_STREAM_WRITER:
        #     BufferedTypeStreamWriterExample(config_).run()


@app.command(
    name="bq-init",
    help="📊 Create dataset and tables in BigQuery if needed",
    no_args_is_help=False,
)
def bigquery_init(
    path_to_config: Annotated[str, typer.Option(help="Path to config file")] = "conf.yaml",
):
    config_ = _load_config(path_to_config)
    PrepareBigQueryService(config_).prepare()
    logger.info("✅ BigQuery infrastructure prepared!")


@app.command(
    name="generate-proto",
    help="📊 Generate proto file from bigquery schema",
    no_args_is_help=False,
)
def generate_proto(
    path_to_bigquery_schema: Annotated[str, typer.Argument(help="Path to bigquery schema file")],
    output_file: Annotated[str, typer.Argument(help="Path to output proto file")],
):
    logger.info(f"📊 Generating proto file from bigquery schema: {path_to_bigquery_schema}")
    logger.info(f"📊 Output file: {output_file}")

    source = Path(path_to_bigquery_schema).resolve()
    if not source.exists() or not source.is_file() or source.suffix != ".json":
        raise FileNotFoundError(f"🛑 Schema file not found: {source}")

    schema = json.load(source.open("r"))
    entity_name = source.stem
    protobuff = ProtoFileGenerator.proto_file(entity_name, schema)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write(protobuff)

    logger.info("✅ Proto file generated!")


def _load_config(path_to_config: str) -> Config:
    _path_to_config = Path(path_to_config).resolve()
    if not _path_to_config.exists():
        raise FileNotFoundError(
            f"Config file at '{path_to_config}' does not exist, please copy conf.yaml.example to conf.yaml and fill in the values"
        )

    with _path_to_config.open("r") as inFile:
        cnf_raw = yaml.safe_load(inFile)
        config_ = Config(**cnf_raw)
        logger.info("✅ Config validation passed!")
        return config_


def entrypoint():
    app()
