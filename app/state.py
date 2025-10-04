from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from app.services.csv_loader import HospitalCSVRow

if TYPE_CHECKING:
    from app.services.bulk_processor import RowProcessingResult

BatchStatusLiteral = Literal[
    "processing",
    "resuming",
    "completed",
    "completed_with_failures",
    "completed_activation_failed",
]


@dataclass(slots=True)
class RowRecord:
    row: int
    name: str
    status: str
    hospital_id: int | None = None
    error: str | None = None


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class BatchSnapshot:
    batch_id: UUID
    status: BatchStatusLiteral
    total: int
    processed: int = 0
    failed: int = 0
    started_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    processing_time_seconds: float | None = None
    batch_activated: bool | None = None
    activation_error: str | None = None
    hospitals: list[RowRecord] = field(default_factory=list)
    failed_rows: list[HospitalCSVRow] = field(default_factory=list)

    def clone(self) -> BatchSnapshot:
        """Return a deep copy so external callers cannot mutate internal state."""
        return copy.deepcopy(self)


class BatchStore:
    """In-memory batch progress tracking for uploads."""

    def __init__(self) -> None:
        self._batches: dict[UUID, BatchSnapshot] = {}
        self._lock = asyncio.Lock()

    async def reset(self) -> None:
        async with self._lock:
            self._batches.clear()

    async def begin_batch(self, batch_id: UUID, total: int) -> None:
        async with self._lock:
            snapshot = BatchSnapshot(batch_id=batch_id, status="processing", total=total)
            self._batches[batch_id] = snapshot

    async def start_resume(self, batch_id: UUID) -> None:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            snapshot.status = "resuming"
            snapshot.updated_at = _utcnow()

    async def record_row(
        self,
        batch_id: UUID,
        row_result: RowProcessingResult,
        *,
        source_row: HospitalCSVRow | None = None,
    ) -> None:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            # Replace existing record for this row
            snapshot.hospitals = [
                record for record in snapshot.hospitals if record.row != row_result.row
            ]
            snapshot.hospitals.append(
                RowRecord(
                    row=row_result.row,
                    name=row_result.name,
                    status=row_result.status,
                    hospital_id=row_result.hospital_id,
                    error=row_result.error,
                )
            )
            snapshot.hospitals.sort(key=lambda record: record.row)

            if row_result.status == "failed" and source_row is not None:
                snapshot.failed_rows = [
                    row for row in snapshot.failed_rows if row.row_number != source_row.row_number
                ]
                snapshot.failed_rows.append(source_row)
            elif row_result.status != "failed":
                snapshot.failed_rows = [
                    row for row in snapshot.failed_rows if row.row_number != row_result.row
                ]

            snapshot.processed = sum(
                1 for record in snapshot.hospitals if record.status in {"created", "created_and_activated"}
            )
            snapshot.failed = sum(1 for record in snapshot.hospitals if record.status == "failed")
            snapshot.updated_at = _utcnow()

    async def mark_activated(self, batch_id: UUID) -> None:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            snapshot.hospitals = [
                RowRecord(
                    row=record.row,
                    name=record.name,
                    status="created_and_activated"
                    if record.status in {"created", "created_and_activated"}
                    else record.status,
                    hospital_id=record.hospital_id,
                    error=record.error,
                )
                for record in snapshot.hospitals
            ]
            snapshot.batch_activated = True
            snapshot.activation_error = None
            snapshot.processed = sum(
                1 for record in snapshot.hospitals if record.status in {"created", "created_and_activated"}
            )
            snapshot.updated_at = _utcnow()

    async def mark_activation_failure(self, batch_id: UUID, error: str) -> None:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            snapshot.batch_activated = False
            snapshot.activation_error = error
            snapshot.updated_at = _utcnow()

    async def complete_batch(
        self,
        batch_id: UUID,
        *,
        processing_time_seconds: float,
    ) -> BatchSnapshot:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            snapshot.processing_time_seconds = (snapshot.processing_time_seconds or 0) + processing_time_seconds
            snapshot.updated_at = _utcnow()
            if snapshot.failed > 0:
                snapshot.status = "completed_with_failures"
            elif snapshot.batch_activated is False and snapshot.activation_error:
                snapshot.status = "completed_activation_failed"
            else:
                snapshot.status = "completed"
            return snapshot.clone()

    async def get_snapshot(self, batch_id: UUID) -> BatchSnapshot | None:
        async with self._lock:
            snapshot = self._batches.get(batch_id)
            if snapshot is None:
                return None
            return snapshot.clone()

    async def get_failed_rows(self, batch_id: UUID) -> list[HospitalCSVRow]:
        async with self._lock:
            snapshot = self._get_existing(batch_id)
            return copy.deepcopy(snapshot.failed_rows)

    def _get_existing(self, batch_id: UUID) -> BatchSnapshot:
        snapshot = self._batches.get(batch_id)
        if snapshot is None:
            raise KeyError(f"Unknown batch_id: {batch_id}")
        return snapshot


_global_store = BatchStore()


def get_batch_store() -> BatchStore:
    return _global_store
