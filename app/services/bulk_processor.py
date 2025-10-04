from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
import time
from typing import TYPE_CHECKING
import uuid

from app.exceptions import (
    BatchNotFoundError,
    BatchStateUnavailableError,
    NoFailedRowsError,
    RemoteAPIError,
)
from app.services.csv_loader import HospitalCSVRow, parse_hospital_csv
from app.state import BatchStore

if TYPE_CHECKING:
    from app.services.hospital_api import HospitalDirectoryClient
    from app.state import BatchSnapshot


@dataclass(slots=True)
class RowProcessingResult:
    row: int
    name: str
    hospital_id: int | None
    status: str
    error: str | None = None


@dataclass(slots=True)
class BulkProcessingResult:
    batch_id: uuid.UUID
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    activation_error: str | None = None
    hospitals: list[RowProcessingResult] = field(default_factory=list)


class BulkProcessingService:
    """Orchestrate CSV parsing and upstream API interactions."""

    def __init__(
        self,
        *,
        row_limit: int,
        client_factory: Callable[[], HospitalDirectoryClient],
        batch_store: BatchStore | None = None,
    ) -> None:
        self._row_limit = row_limit
        self._client_factory = client_factory
        self._batch_store = batch_store

    async def process_upload(self, raw_bytes: bytes) -> BulkProcessingResult:
        start = time.perf_counter()
        rows = parse_hospital_csv(raw_bytes, limit=self._row_limit)
        batch_id = uuid.uuid4()

        store = self._batch_store
        if store is not None:
            await store.begin_batch(batch_id, len(rows))

        hospitals: list[RowProcessingResult] = []
        successes = 0
        failures = 0
        activation_error: str | None = None
        batch_activated = False

        async with self._client_factory() as client:
            for row in rows:
                try:
                    hospitals.append(await self._process_row(client, batch_id, row))
                except RemoteAPIError as exc:
                    failures += 1
                    failure_result = RowProcessingResult(
                        row=row.row_number,
                        name=row.name,
                        hospital_id=None,
                        status="failed",
                        error=str(exc),
                    )
                    hospitals.append(failure_result)
                    if store is not None:
                        await store.record_row(batch_id, failure_result, source_row=row)
                else:
                    successes += 1
                    if store is not None:
                        await store.record_row(batch_id, hospitals[-1])

            if failures == 0 and hospitals:
                try:
                    await client.activate_batch(batch_id)
                except RemoteAPIError as exc:  # Activation failed but creations remain
                    activation_error = str(exc)
                    batch_activated = False
                    if store is not None:
                        await store.mark_activation_failure(batch_id, activation_error)
                else:
                    batch_activated = True
                    for result in hospitals:
                        if result.status == "created":
                            result.status = "created_and_activated"
                    if store is not None:
                        await store.mark_activated(batch_id)

        elapsed = time.perf_counter() - start

        if store is not None:
            snapshot = await store.complete_batch(batch_id, processing_time_seconds=elapsed)
            return self._snapshot_to_result(snapshot)

        return BulkProcessingResult(
            batch_id=batch_id,
            total_hospitals=len(rows),
            processed_hospitals=successes,
            failed_hospitals=failures,
            processing_time_seconds=round(elapsed, 3),
            batch_activated=batch_activated,
            activation_error=activation_error,
            hospitals=hospitals,
        )

    async def resume_failed_batch(self, batch_id: uuid.UUID) -> BulkProcessingResult:
        store = self._batch_store
        if store is None:
            raise BatchStateUnavailableError()

        snapshot = await store.get_snapshot(batch_id)
        if snapshot is None:
            raise BatchNotFoundError(batch_id)

        failed_rows = await store.get_failed_rows(batch_id)
        if not failed_rows:
            raise NoFailedRowsError(batch_id)

        await store.start_resume(batch_id)

        start = time.perf_counter()

        async with self._client_factory() as client:
            for row in failed_rows:
                try:
                    result = await self._process_row(client, batch_id, row)
                except RemoteAPIError as exc:
                    failure_result = RowProcessingResult(
                        row=row.row_number,
                        name=row.name,
                        hospital_id=None,
                        status="failed",
                        error=str(exc),
                    )
                    await store.record_row(batch_id, failure_result, source_row=row)
                else:
                    await store.record_row(batch_id, result)

            updated_snapshot = await store.get_snapshot(batch_id)
            if updated_snapshot is None:
                raise BatchNotFoundError(batch_id)

            if updated_snapshot.failed == 0 and updated_snapshot.total > 0:
                try:
                    await client.activate_batch(batch_id)
                except RemoteAPIError as exc:
                    await store.mark_activation_failure(batch_id, str(exc))
                else:
                    await store.mark_activated(batch_id)

        elapsed = time.perf_counter() - start
        final_snapshot = await store.complete_batch(batch_id, processing_time_seconds=elapsed)
        return self._snapshot_to_result(final_snapshot)

    async def _process_row(
        self,
        client: HospitalDirectoryClient,
        batch_id: uuid.UUID,
        row: HospitalCSVRow,
    ) -> RowProcessingResult:
        response = await client.create_hospital(
            name=row.name,
            address=row.address,
            phone=row.phone,
            creation_batch_id=batch_id,
        )
        hospital_id = None
        if isinstance(response, dict):
            hospital_id = response.get("id")
        return RowProcessingResult(
            row=row.row_number,
            name=row.name,
            hospital_id=hospital_id,
            status="created",
        )

    def _snapshot_to_result(self, snapshot: BatchSnapshot) -> BulkProcessingResult:
        processing_time = snapshot.processing_time_seconds or 0.0
        return BulkProcessingResult(
            batch_id=snapshot.batch_id,
            total_hospitals=snapshot.total,
            processed_hospitals=snapshot.processed,
            failed_hospitals=snapshot.failed,
            processing_time_seconds=round(processing_time, 3),
            batch_activated=bool(snapshot.batch_activated),
            activation_error=snapshot.activation_error,
            hospitals=[
                RowProcessingResult(
                    row=record.row,
                    name=record.name,
                    hospital_id=record.hospital_id,
                    status=record.status,
                    error=record.error,
                )
                for record in sorted(snapshot.hospitals, key=lambda record: record.row)
            ],
        )
