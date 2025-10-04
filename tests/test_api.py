from __future__ import annotations

import json

import httpx
import pytest

from app.config import Settings
from app.main import app
from app.routes import bulk
from app.services.hospital_api import HospitalDirectoryClient
from app.state import BatchStore, get_batch_store


@pytest.fixture(autouse=True)
def clear_overrides():
    original = dict(app.dependency_overrides)
    yield
    app.dependency_overrides = original


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


@pytest.mark.asyncio
async def test_bulk_endpoint_success():
    fake_api = FakeHospitalDirectoryAPI()
    store = BatchStore()

    def override_client_factory():
        return lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        )

    app.dependency_overrides[bulk.provide_client_factory] = override_client_factory
    app.dependency_overrides[get_batch_store] = lambda: store

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/hospitals/bulk",
            files={"file": ("hospitals.csv", _sample_csv(2), "text/csv")},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["total_hospitals"] == 2
    assert body["processed_hospitals"] == 2
    assert body["failed_hospitals"] == 0
    assert body["batch_activated"] is True
    assert len(body["hospitals"]) == 2
    assert fake_api.activation_called is True

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        status_response = await client.get(f"/hospitals/bulk/{body['batch_id']}")

    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["status"] == "completed"
    assert status_body["processed_hospitals"] == 2
    assert status_body["failed_hospitals"] == 0


@pytest.mark.asyncio
async def test_bulk_endpoint_row_limit_exceeded():
    fake_api = FakeHospitalDirectoryAPI()
    store = BatchStore()

    def override_client_factory():
        return lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        )

    app.dependency_overrides[bulk.provide_client_factory] = override_client_factory
    app.dependency_overrides[get_batch_store] = lambda: store
    app.dependency_overrides[bulk.get_settings] = lambda: Settings(
        hospital_directory_api_base_url="https://hospital-directory.test",
        batch_size_limit=1,
        outbound_timeout_seconds=5,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/hospitals/bulk",
            files={"file": ("hospitals.csv", _sample_csv(2), "text/csv")},
        )

    assert response.status_code == 400
    body = response.json()
    assert body["detail"] == "CSV contains more rows than allowed."
    assert body["limit"] == 1
    assert body["actual"] == 2


@pytest.mark.asyncio
async def test_resume_endpoint_success():
    fake_api = FakeHospitalDirectoryAPI(fail_on_second=True)
    store = BatchStore()

    def override_client_factory():
        return lambda: HospitalDirectoryClient(
            base_url="https://hospital-directory.test",
            timeout=5,
            transport=httpx.MockTransport(fake_api.handler),
        )

    app.dependency_overrides[bulk.provide_client_factory] = override_client_factory
    app.dependency_overrides[get_batch_store] = lambda: store

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/hospitals/bulk",
            files={"file": ("hospitals.csv", _sample_csv(2), "text/csv")},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["failed_hospitals"] == 1
    assert fake_api.activation_called is False

    batch_id = body["batch_id"]

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        resume_response = await client.post(f"/hospitals/bulk/{batch_id}/resume")

    assert resume_response.status_code == 200
    resume_body = resume_response.json()
    assert resume_body["failed_hospitals"] == 0
    assert resume_body["processed_hospitals"] == 2
    assert resume_body["batch_activated"] is True
    assert fake_api.activation_called is True

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        status_response = await client.get(f"/hospitals/bulk/{batch_id}")

    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["status"] == "completed"
    assert status_body["failed_hospitals"] == 0


def _sample_csv(count: int) -> str:
    rows = ["name,address,phone"]
    for idx in range(count):
        rows.append(f"Hospital {idx+1},123 Main St,555-000{idx}")
    return "\n".join(rows)
