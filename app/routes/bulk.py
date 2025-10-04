from __future__ import annotations

from collections.abc import Callable
import logging
from typing import TYPE_CHECKING, Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse

from app.config import Settings, get_settings
from app.exceptions import (
    BatchNotFoundError,
    BatchStateUnavailableError,
    CSVFormatError,
    CSVTooLargeError,
    NoFailedRowsError,
)
from app.schemas import (
    BatchProgressResponse,
    BulkProcessingResponse,
    CSVSizeErrorResponse,
    CSVValidationErrorResponse,
    GenericErrorResponse,
    RowStatus,
)
from app.services.bulk_processor import BulkProcessingService
from app.services.hospital_api import HospitalDirectoryClient
from app.state import BatchStore, get_batch_store

if TYPE_CHECKING:
    from app.services.bulk_processor import BulkProcessingResult
    from app.state import BatchSnapshot

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hospitals", tags=["Hospitals"])

ACCEPTED_CONTENT_TYPES = {"text/csv", "application/csv", "application/vnd.ms-excel"}


def provide_client_factory(
    settings: Annotated[Settings, Depends(get_settings)],
) -> Callable[[], HospitalDirectoryClient]:
    def factory() -> HospitalDirectoryClient:
        return HospitalDirectoryClient(
            base_url=settings.hospital_directory_api_base_url,
            timeout=settings.outbound_timeout_seconds,
        )

    return factory


@router.post(
    "/bulk",
    response_model=BulkProcessingResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "content": {
                "application/json": {
                    "schema": {
                        "oneOf": [
                            CSVValidationErrorResponse.model_json_schema(),
                            CSVSizeErrorResponse.model_json_schema(),
                        ]
                    },
                    "examples": {
                        "validation": {
                            "summary": "Validation failure",
                            "value": {
                                "detail": "Invalid CSV format.",
                                "errors": [
                                    {"row": 1, "message": "Name is required"}
                                ],
                            },
                        },
                        "size": {
                            "summary": "Row limit exceeded",
                            "value": {
                                "detail": "CSV contains more rows than allowed.",
                                "limit": 20,
                                "actual": 25,
                            },
                        },
                    },
                }
            },
            "description": "CSV validation failure or row limit exceeded",
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": GenericErrorResponse,
            "description": "Internal server error",
        },
    },
)
async def upload_bulk_hospitals(
    file: Annotated[UploadFile, File(..., description="CSV file containing hospital rows")],
    settings: Annotated[Settings, Depends(get_settings)],
    client_factory: Annotated[Callable[[], HospitalDirectoryClient], Depends(provide_client_factory)],
    batch_store: Annotated[BatchStore, Depends(get_batch_store)],
) -> BulkProcessingResponse:
    if file.content_type and file.content_type.lower() not in ACCEPTED_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported content type: {file.content_type}",
        )

    contents = await file.read()
    service = BulkProcessingService(
        row_limit=settings.batch_size_limit,
        client_factory=client_factory,
        batch_store=batch_store,
    )

    try:
        result = await service.process_upload(contents)
    except CSVTooLargeError as exc:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=CSVSizeErrorResponse(
                detail="CSV contains more rows than allowed.",
                limit=exc.limit,
                actual=exc.actual,
            ).model_dump(),
        )
    except CSVFormatError as exc:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=CSVValidationErrorResponse(
                detail="Invalid CSV format.",
                errors=[{"row": err.row_number, "message": err.message} for err in exc.errors],
            ).model_dump(),
        )
    except Exception as exc:  # noqa: BLE001 - log unexpected errors
        logger.exception("Unhandled error while processing hospital bulk upload")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error.",
        ) from exc

    return _result_to_response(result)


@router.get(
    "/bulk/{batch_id}",
    response_model=BatchProgressResponse,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": GenericErrorResponse,
            "description": "Batch not found",
        }
    },
)
async def get_bulk_batch_status(
    batch_id: UUID,
    batch_store: Annotated[BatchStore, Depends(get_batch_store)],
) -> BatchProgressResponse:
    snapshot = await batch_store.get_snapshot(batch_id)
    if snapshot is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found.")
    return _snapshot_to_progress(snapshot)


@router.post(
    "/bulk/{batch_id}/resume",
    response_model=BulkProcessingResponse,
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "model": GenericErrorResponse,
            "description": "Batch cannot be resumed",
        },
        status.HTTP_404_NOT_FOUND: {
            "model": GenericErrorResponse,
            "description": "Batch not found",
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "model": GenericErrorResponse,
            "description": "Internal server error",
        },
    },
)
async def resume_bulk_batch(
    batch_id: UUID,
    settings: Annotated[Settings, Depends(get_settings)],
    client_factory: Annotated[Callable[[], HospitalDirectoryClient], Depends(provide_client_factory)],
    batch_store: Annotated[BatchStore, Depends(get_batch_store)],
) -> BulkProcessingResponse:
    service = BulkProcessingService(
        row_limit=settings.batch_size_limit,
        client_factory=client_factory,
        batch_store=batch_store,
    )

    try:
        result = await service.resume_failed_batch(batch_id)
    except BatchNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found.") from None
    except NoFailedRowsError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No failed rows remain for this batch.",
        ) from None
    except BatchStateUnavailableError:
        logger.exception("Batch store unavailable while resuming batch %s", batch_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch state unavailable.",
        ) from None
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unhandled error while resuming hospital bulk batch %s", batch_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error.",
        ) from exc

    return _result_to_response(result)


def _result_to_response(result: BulkProcessingResult) -> BulkProcessingResponse:
    return BulkProcessingResponse(
        batch_id=result.batch_id,
        total_hospitals=result.total_hospitals,
        processed_hospitals=result.processed_hospitals,
        failed_hospitals=result.failed_hospitals,
        processing_time_seconds=result.processing_time_seconds,
        batch_activated=result.batch_activated,
        activation_error=result.activation_error,
        hospitals=[
            RowStatus(
                row=row.row,
                name=row.name,
                status=row.status,
                hospital_id=row.hospital_id,
                error=row.error,
            )
            for row in sorted(result.hospitals, key=lambda r: r.row)
        ],
    )


def _snapshot_to_progress(snapshot: BatchSnapshot) -> BatchProgressResponse:
    processing_time = snapshot.processing_time_seconds
    return BatchProgressResponse(
        batch_id=snapshot.batch_id,
        status=snapshot.status,
        total_hospitals=snapshot.total,
        processed_hospitals=snapshot.processed,
        failed_hospitals=snapshot.failed,
        started_at=snapshot.started_at,
        updated_at=snapshot.updated_at,
        processing_time_seconds=round(processing_time, 3) if processing_time is not None else None,
        batch_activated=snapshot.batch_activated,
        activation_error=snapshot.activation_error,
        hospitals=[
            RowStatus(
                row=record.row,
                name=record.name,
                status=record.status,
                hospital_id=record.hospital_id,
                error=record.error,
            )
            for record in sorted(snapshot.hospitals, key=lambda record: record.row)
        ],
    )
