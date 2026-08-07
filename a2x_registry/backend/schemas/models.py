"""Pydantic models for API request/response schemas."""

from pydantic import BaseModel

class DatasetInfo(BaseModel):
    name: str
    service_count: int
