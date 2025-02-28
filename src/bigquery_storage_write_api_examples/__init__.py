from pydantic import BaseModel, ConfigDict


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    gcp_project_id: str
    gcp_dataset_id: str
