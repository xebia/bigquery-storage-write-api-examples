from pydantic import BaseModel, ConfigDict

__version__ = "0.0.0"


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    gcp_project_id: str
    gcp_dataset_id: str
