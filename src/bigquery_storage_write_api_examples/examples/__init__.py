import logging
from abc import ABC, abstractmethod

from google.cloud import bigquery


class BigQueryWriterExample(ABC):
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def run(self): ...
