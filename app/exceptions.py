from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


class BulkProcessingError(RuntimeError):
    """Base error for bulk processing pipeline."""


class CSVTooLargeError(BulkProcessingError):
    def __init__(self, *, limit: int, actual: int) -> None:
        super().__init__(f"CSV row limit exceeded: limit={limit}, actual={actual}")
        self.limit = limit
        self.actual = actual


@dataclass(slots=True)
class CSVRowError:
    row_number: int
    message: str


class CSVFormatError(BulkProcessingError):
    def __init__(self, errors: list[CSVRowError]):
        detail = "; ".join(f"row {err.row_number}: {err.message}" for err in errors)
        super().__init__(f"Invalid CSV content - {detail}")
        self.errors = errors


class RemoteAPIError(BulkProcessingError):
    def __init__(self, status_code: int, message: str | None = None) -> None:
        detail = message or "Remote API call failed"
        super().__init__(f"[{status_code}] {detail}")
        self.status_code = status_code
        self.detail = detail


class BatchNotFoundError(BulkProcessingError):
    def __init__(self, batch_id: UUID) -> None:
        super().__init__(f"Batch not found: {batch_id}")
        self.batch_id = batch_id


class NoFailedRowsError(BulkProcessingError):
    def __init__(self, batch_id: UUID) -> None:
        super().__init__(f"No failed rows remain for batch: {batch_id}")
        self.batch_id = batch_id


class BatchStateUnavailableError(BulkProcessingError):
    def __init__(self) -> None:
        super().__init__("Batch state store is not configured")
