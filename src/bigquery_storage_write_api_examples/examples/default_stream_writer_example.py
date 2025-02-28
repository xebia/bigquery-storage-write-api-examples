from bigquery_storage_write_api_examples import Config
from bigquery_storage_write_api_examples.examples import BigQueryWriterExample


class DefaultStreamWriterExample(BigQueryWriterExample):
    def __init__(self, config: Config):
        self.project_id = config.gcp_project_id
        self.dataset_id = config.gcp_dataset_id
        self.table_id = config.gcp_table_id
        super().__init__(self.project_id)

    def run(self):
        pass
