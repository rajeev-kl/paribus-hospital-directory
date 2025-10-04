from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI

# ProxyHeadersMiddleware was previously available as
# `starlette.middleware.proxy_headers.ProxyHeadersMiddleware` but may be
# missing in some Starlette versions. Try the canonical import first and
# fall back to a small no-op compatible shim so tests and local runtimes
# that don't require proxy header handling still work.
try:
    from starlette.middleware.proxy_headers import ProxyHeadersMiddleware
except Exception:  # pragma: no cover - fallback for older/newer starlette
    class ProxyHeadersMiddleware:  # type: ignore[misc]
        """A minimal no-op replacement for Starlette's ProxyHeadersMiddleware.

        The real middleware updates the request scope (client/scheme) from
        X-Forwarded-* headers. Tests in this project don't rely on that
        behavior, so a pass-through implementation is safe and keeps the
        import stable across Starlette versions.
        """

        def __init__(self, app, trusted_hosts=None):
            self._app = app

        async def __call__(self, scope, receive, send):
            await self._app(scope, receive, send)

from app.config import get_settings
from app.routes.bulk import router as bulk_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    logger.info(
        "Starting bulk processor with base_url=%s, row_limit=%s",
        settings.hospital_directory_api_base_url,
        settings.batch_size_limit,
    )
    yield


app = FastAPI(
    title="Paribus Hospital Bulk Processor",
    description="Bulk CSV ingestion service that feeds the Hospital Directory API.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(bulk_router)

# Trust headers set by the upstream nginx proxy (X-Forwarded-For, X-Forwarded-Proto)
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["*"])

# Configure root logger level from environment (defaults to INFO in production)
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(log_level)


@app.get("/", tags=["Health"], summary="Health check")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
