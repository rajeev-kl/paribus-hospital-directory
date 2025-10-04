from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

BatchStatus = Literal[
    "processing",
    "resuming",
    "completed",
    "completed_with_failures",
    "completed_activation_failed",
]


class RowStatus(BaseModel):
    row: int
    name: str
    status: Literal["created", "created_and_activated", "failed"]
    hospital_id: int | None = Field(default=None, description="Hospital ID returned by upstream API")
    error: str | None = Field(default=None, description="Error message when the row fails")

    model_config = {
        "json_schema_extra": {
            "example": {
                "row": 1,
                "name": "General Hospital",
                "status": "created_and_activated",
                "hospital_id": 101,
            }
        }
    }


class BulkProcessingResponse(BaseModel):
    batch_id: UUID
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    activation_error: str | None = None
    hospitals: list[RowStatus]

    model_config = {
        "json_schema_extra": {
            "example": {
                "batch_id": "550e8400-e29b-41d4-a716-446655440000",
                "total_hospitals": 2,
                "processed_hospitals": 2,
                "failed_hospitals": 0,
                "processing_time_seconds": 1.234,
                "batch_activated": True,
                "hospitals": [
                    {
                        "row": 1,
                        "name": "General Hospital",
                        "status": "created_and_activated",
                        "hospital_id": 101,
                    }
                ],
            }
        }
    }


class BatchProgressResponse(BaseModel):
    batch_id: UUID
    status: BatchStatus
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    started_at: datetime
    updated_at: datetime
    processing_time_seconds: float | None = Field(
        default=None, description="Total processing time accumulated across attempts."
    )
    batch_activated: bool | None = Field(
        default=None,
        description="Whether activation succeeded. None indicates activation not attempted or unknown.",
    )
    activation_error: str | None = Field(default=None, description="Latest activation failure message, if any.")
    hospitals: list[RowStatus]

    model_config = {
        "json_schema_extra": {
            "example": {
                "batch_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "completed",
                "total_hospitals": 2,
                "processed_hospitals": 2,
                "failed_hospitals": 0,
                "started_at": "2024-01-01T10:00:00Z",
                "updated_at": "2024-01-01T10:00:01Z",
                "processing_time_seconds": 1.234,
                "batch_activated": True,
                "hospitals": [
                    {
                        "row": 1,
                        "name": "General Hospital",
                        "status": "created_and_activated",
                        "hospital_id": 101,
                    }
                ],
            }
        }
    }


class CSVValidationErrorDetail(BaseModel):
    row: int
    message: str


class CSVValidationErrorResponse(BaseModel):
    detail: str
    errors: list[CSVValidationErrorDetail]


class CSVSizeErrorResponse(BaseModel):
    detail: str
    limit: int
    actual: int


class GenericErrorResponse(BaseModel):
    detail: str
