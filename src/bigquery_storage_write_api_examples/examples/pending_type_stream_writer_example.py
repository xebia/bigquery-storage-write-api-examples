from bigquery_storage_write_api_examples.examples import BigQueryWriterExample


class PendingTypeStreamWriterExample(BigQueryWriterExample):
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        super().__init__(project_id, dataset_id, table_id)

    def run(self):
        pass
