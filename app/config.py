import os
from dataclasses import dataclass

from dotenv import load_dotenv


_env_file = os.getenv("ENV_FILE", ".env")
load_dotenv(_env_file)


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Environment variable {name} must be an integer") from exc


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_key: str
    api_title: str
    api_version: str
    default_timeline_limit: int
    default_events_limit: int
    max_events_lookback_rows: int


settings = Settings(
    supabase_url=_env("SUPABASE_URL"),
    supabase_key=_env("SUPABASE_KEY"),
    api_title=_env("API_TITLE", "taxonomy-product-api"),
    api_version=_env("API_VERSION", "0.1.0"),
    default_timeline_limit=_env_int("DEFAULT_TIMELINE_LIMIT", 8),
    default_events_limit=_env_int("DEFAULT_EVENTS_LIMIT", 50),
    max_events_lookback_rows=_env_int("MAX_EVENTS_LOOKBACK_ROWS", 1200),
)
