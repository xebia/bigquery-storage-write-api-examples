import json
import logging
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from bigquery_storage_write_api_examples import Config


class PrepareBigQueryService:
    """
    Prepares BigQuery infrastructure by setting up dataset and tables.

    Performs the following setup:
        - Creates a new BigQuery dataset if one doesn't already exist
        - Creates all required tables using schema definitions from the 'misc/schemas' directory
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger("bigquery_preparation")
        self.logger.setLevel(logging.INFO)

    def prepare(self):
        logging.info("Preparing BigQuery infrastructure...")
        client = bigquery.Client(project=self.config.gcp_project_id)

        dataset = self._create_dataset(client, self.config.gcp_dataset_id)

        schemas_dir = Path("./misc/schemas").resolve()
        for schema in Path(schemas_dir).glob("*.json"):
            with schema.open("r") as f:
                schema_data = json.load(f)
                table_name = schema.stem
                _table = self._create_table(client, dataset, table_name, schema_data)

    def _create_dataset(self, client: bigquery.Client, dataset_id: str):
        """
        Creates a new BigQuery dataset if it doesn't already exist.

        Args:
            client: The BigQuery client object
            dataset_id: The ID of the dataset to create
        """
        try:
            self.logger.info(f"Checking if dataset {dataset_id} exists")
            dataset = client.get_dataset(dataset_id)
            self.logger.info(f"✅ Dataset {dataset_id} found")
            return dataset
        except NotFound:
            # Why doesn't the BigQuery client library include a simple exists()
            # method for datasets and tables?
            # The current try-except approach feels cumbersome.
            self.logger.info(f"⭕ Dataset {dataset_id} not found, creating it...")
            dataset = client.create_dataset(dataset_id)
            self.logger.info(f"✅ Dataset {dataset_id} created")
            return dataset

    def _create_table(
        self, client: bigquery.Client, dataset: bigquery.Dataset, table_name: str, schema: dict
    ):
        """
            Creates a new table in BigQuery if it doesn't already exist.

        Args:
            client (bigquery.Client): _description_
            dataset (bigquery.Dataset): _description_
            table_name (str): _description_
            schema (dict): _description_
        """
        try:
            table = client.get_table(f"{dataset.dataset_id}.{table_name}")
            self.logger.info(f"✅ Table '{table_name}' found")
            return table
        except NotFound:
            self.logger.info(f"⭕ Table '{table_name}' not found, creating it...")
            table = bigquery.Table(f"{dataset.project}.{dataset.dataset_id}.{table_name}", schema=schema)
            table = client.create_table(table)
            self.logger.info(f"✅ Table '{table_name}' created")
            return table
