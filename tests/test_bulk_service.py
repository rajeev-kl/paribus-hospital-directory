from __future__ import annotations

import json

import httpx
import pytest

from app.exceptions import NoFailedRowsError
from app.services.bulk_processor import BulkProcessingService
from app.services.hospital_api import HospitalDirectoryClient
from app.state import BatchStore


@pytest.mark.asyncio
async def test_process_upload_success() -> None:
    csv_content = """name,address,phone\nGeneral Hospital,123 Main St,555-1234\nCity Clinic,456 Side St,\n"""
    fake_api = FakeHospitalDirectoryAPI()
    store = BatchStore()
    service = BulkProcessingService(
        row_limit=5,
        client_factory=lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        ),
        batch_store=store,
    )

    result = await service.process_upload(csv_content.encode("utf-8"))

    assert result.total_hospitals == 2
    assert result.processed_hospitals == 2
    assert result.failed_hospitals == 0
    assert result.batch_activated is True
    assert result.activation_error is None
    assert all(row.status == "created_and_activated" for row in result.hospitals)
    assert len(fake_api.created_payloads) == 2
    assert all(payload["creation_batch_id"] == str(result.batch_id) for payload in fake_api.created_payloads)
    snapshot = await store.get_snapshot(result.batch_id)
    assert snapshot is not None
    assert snapshot.status == "completed"
    assert snapshot.processed == 2
    assert snapshot.failed == 0


@pytest.mark.asyncio
async def test_process_upload_handles_remote_failure() -> None:
    csv_content = """name,address,phone\nGeneral Hospital,123 Main St,555-1234\nFaulty Hospital,789 Error Rd,555-0000\n"""
    fake_api = FakeHospitalDirectoryAPI(fail_on_second=True)
    store = BatchStore()
    service = BulkProcessingService(
        row_limit=5,
        client_factory=lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        ),
        batch_store=store,
    )

    result = await service.process_upload(csv_content.encode("utf-8"))

    assert result.total_hospitals == 2
    assert result.processed_hospitals == 1
    assert result.failed_hospitals == 1
    assert result.batch_activated is False
    assert result.activation_error is None
    assert result.hospitals[1].status == "failed"
    assert result.hospitals[1].error is not None
    assert fake_api.activation_called is False
    snapshot = await store.get_snapshot(result.batch_id)
    assert snapshot is not None
    assert snapshot.status == "completed_with_failures"
    assert snapshot.failed == 1


@pytest.mark.asyncio
async def test_resume_failed_batch_success() -> None:
    csv_content = """name,address,phone\nGeneral Hospital,123 Main St,555-1234\nFaulty Hospital,789 Error Rd,555-0000\n"""
    fake_api = FakeHospitalDirectoryAPI(fail_on_second=True)
    store = BatchStore()
    service = BulkProcessingService(
        row_limit=5,
        client_factory=lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        ),
        batch_store=store,
    )

    initial_result = await service.process_upload(csv_content.encode("utf-8"))
    assert initial_result.failed_hospitals == 1

    resume_result = await service.resume_failed_batch(initial_result.batch_id)

    assert resume_result.failed_hospitals == 0
    assert resume_result.processed_hospitals == 2
    assert resume_result.batch_activated is True
    assert fake_api.activation_called is True
    snapshot = await store.get_snapshot(initial_result.batch_id)
    assert snapshot is not None
    assert snapshot.status == "completed"
    assert snapshot.failed == 0


@pytest.mark.asyncio
async def test_resume_failed_batch_without_failures_raises() -> None:
    csv_content = """name,address,phone\nGeneral Hospital,123 Main St,555-1234\n"""
    fake_api = FakeHospitalDirectoryAPI()
    store = BatchStore()
    service = BulkProcessingService(
        row_limit=5,
        client_factory=lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        ),
        batch_store=store,
    )

    result = await service.process_upload(csv_content.encode("utf-8"))
    snapshot = await store.get_snapshot(result.batch_id)
    assert snapshot is not None
    assert snapshot.failed == 0

    with pytest.raises(NoFailedRowsError):
        await service.resume_failed_batch(result.batch_id)


class FakeHospitalDirectoryAPI:
    def __init__(self, *, fail_on_second: bool = False) -> None:
        self.created_payloads: list[dict[str, str]] = []
        self.activation_called = False
        self._fail_on_second = fail_on_second
        self._create_count = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/hospitals/":
            self._create_count += 1
            if self._fail_on_second and self._create_count == 2:
                return httpx.Response(status_code=422, json={"detail": "Invalid data"})
            payload = json.loads(request.content.decode())
            self.created_payloads.append(payload)
            return httpx.Response(
                status_code=200,
                json={
                    "id": self._create_count,
                    "name": payload["name"],
                    "address": payload["address"],
                    "creation_batch_id": payload["creation_batch_id"],
                    "active": False,
                },
            )

        if request.url.path.startswith("/hospitals/batch/") and request.url.path.endswith("/activate"):
            self.activation_called = True
            return httpx.Response(status_code=200, json={"status": "activated"})

        return httpx.Response(status_code=404)
