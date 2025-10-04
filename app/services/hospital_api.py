from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import uuid

import httpx

from app.exceptions import RemoteAPIError


class HospitalDirectoryClient:
    """Async client wrapper for the external Hospital Directory API."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if client is not None and transport is not None:
            raise ValueError("Specify either a custom client or transport, not both.")

        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"accept": "application/json"},
            transport=transport,
        )

    async def __aenter__(self) -> HospitalDirectoryClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def create_hospital(
        self,
        *,
        name: str,
        address: str,
        phone: str | None,
        creation_batch_id: uuid.UUID,
    ) -> Mapping[str, Any]:
        payload: dict[str, Any] = {
            "name": name,
            "address": address,
            "creation_batch_id": str(creation_batch_id),
        }
        if phone:
            payload["phone"] = phone

        try:
            response = await self._client.post("/hospitals/", json=payload)
        except httpx.HTTPError as exc:
            raise RemoteAPIError(0, str(exc)) from exc

        if response.status_code >= 400:
            self._raise_error(response)
        return response.json()

    async def activate_batch(self, batch_id: uuid.UUID) -> Mapping[str, Any] | None:
        try:
            response = await self._client.patch(f"/hospitals/batch/{batch_id}/activate")
        except httpx.HTTPError as exc:
            raise RemoteAPIError(0, str(exc)) from exc

        if response.status_code >= 400:
            self._raise_error(response)

        if response.headers.get("content-type", "").startswith("application/json") and response.content:
            return response.json()
        return None

    @staticmethod
    def _raise_error(response: httpx.Response) -> None:
        detail: str | None = None
        try:
            body = response.json()
        except ValueError:
            detail = response.text or None
        else:
            if isinstance(body, Mapping) and "detail" in body:
                detail_value = body["detail"]
                if isinstance(detail_value, str):
                    detail = detail_value
                elif isinstance(detail_value, list) and detail_value:
                    detail = str(detail_value[0])
        raise RemoteAPIError(response.status_code, detail)
