from abc import ABC, abstractmethod

from google.cloud import bigquery


class BigQueryWriterExample(ABC):
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    @abstractmethod
    def run(self): ...
