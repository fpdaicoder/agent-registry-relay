"""Pydantic models for API request/response schemas."""

from typing import Dict, List, Optional

from pydantic import BaseModel

class DatasetInfo(BaseModel):
    name: str
    service_count: int


class SearchRequest(BaseModel):
    query: str
    method: str
    dataset: str = "ToolRet_clean"
    top_k: Optional[int] = 10


class ServiceResult(BaseModel):
    id: str
    name: str
    description: str


class SearchResponse(BaseModel):
    results: List[ServiceResult]
    stats: Dict
    elapsed_time: float


class JudgeRequest(BaseModel):
    query: str
    services: List[ServiceResult]


class JudgeResult(BaseModel):
    service_id: str
    relevant: bool


class JudgeResponse(BaseModel):
    results: List[JudgeResult]
