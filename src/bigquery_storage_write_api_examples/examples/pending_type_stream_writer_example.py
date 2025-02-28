from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.examples import BigQueryWriterExample


# courses
class PendingTypeStreamWriterExample(BigQueryWriterExample):
    """
    This example demonstrates how to insert data into a BigQuery table using the Pending Type Stream Writer.

    Use this writer when you want to add data to a table and commit it at a later time; until you commit,
    the data will not appear in query results.

    For more information, see:
        - https://cloud.google.com/bigquery/docs/write-api-batch
    """

    def __init__(self, config: Config):
        super().__init__(project_id=config.gcp_project_id)
        self.dataset_id = config.gcp_bigquery_dataset_id
        self.table_id = config.gcp_bigquery_table_id

    def _init_stream(self):
        pass
        # self.stream = self.client.create_write_stream(
        #     parent=f"projects/{self.project_id}/datasets/{self.dataset_id}/tables/{self.table_id}",
        #     write_stream=WriteStream(
        #         write_stream_id=f"{self.table_id}-{uuid.uuid4()}",
        #     ),
        # )

    def run(self):
        pass
