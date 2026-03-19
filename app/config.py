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


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_key: str
    api_title: str
    api_version: str


settings = Settings(
    supabase_url=_env("SUPABASE_URL"),
    supabase_key=_env("SUPABASE_KEY"),
    api_title=_env("API_TITLE", "taxonomy-product-api"),
    api_version=_env("API_VERSION", "0.1.0"),
)
