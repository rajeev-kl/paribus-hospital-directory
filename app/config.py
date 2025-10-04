from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    hospital_directory_api_base_url: str = "https://hospital-directory.onrender.com"
    batch_size_limit: int = 20
    outbound_timeout_seconds: float = 10.0
    # When the application is exposed under a path prefix by a reverse proxy
    # (e.g. https://example.com/paribus/ -> upstream /), set ROOT_PATH to that
    # prefix ("/paribus") so FastAPI generates correct OpenAPI/Docs asset URLs.
    root_path: str = ""


class RuntimeState(BaseModel):
    settings: Settings


@lru_cache
def get_settings() -> Settings:
    return Settings()
