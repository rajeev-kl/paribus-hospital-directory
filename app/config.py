from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    hospital_directory_api_base_url: str = "https://hospital-directory.onrender.com"
    batch_size_limit: int = 20
    outbound_timeout_seconds: float = 10.0


class RuntimeState(BaseModel):
    settings: Settings


@lru_cache
def get_settings() -> Settings:
    return Settings()
