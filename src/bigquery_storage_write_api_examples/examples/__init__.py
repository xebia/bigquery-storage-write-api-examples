from abc import ABC, abstractmethod

# from google.cloud import bigquery


class BigQueryWriterExample(ABC):
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        # self.client = bigquery.Client(project=project_id)
        pass

    @abstractmethod
    def run(self):
        pass
