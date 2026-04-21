from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class EndpointConfig:
    base_url: str
    instance_id: str
    api_key: str
    requests_per_second: float


@dataclass(frozen=True)
class RetryConfig:
    retry_count: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    backoff_jitter_seconds: float


@dataclass(frozen=True)
class Settings:
    source: EndpointConfig
    target: EndpointConfig
    retry: RetryConfig
    export_limit: int | None


def load_settings(env_file: str | Path = ".env") -> Settings:
    _load_dotenv_file(Path(env_file))
    return Settings(
        source=_load_endpoint("MP_SOURCE"),
        target=_load_endpoint("MP_TARGET"),
        retry=RetryConfig(
            retry_count=int(os.environ.get("MP_RETRY_COUNT", "4")),
            backoff_base_seconds=float(os.environ.get("MP_BACKOFF_BASE_SECONDS", "1.0")),
            backoff_max_seconds=float(os.environ.get("MP_BACKOFF_MAX_SECONDS", "15.0")),
            backoff_jitter_seconds=float(os.environ.get("MP_BACKOFF_JITTER_SECONDS", "0.25")),
        ),
        export_limit=_optional_int("MP_EXPORT_LIMIT"),
    )


def _load_endpoint(prefix: str) -> EndpointConfig:
    base_url = _required(f"{prefix}_BASE_URL").rstrip("/")
    return EndpointConfig(
        base_url=base_url,
        instance_id=_required(f"{prefix}_INSTANCE_ID"),
        api_key=_required(f"{prefix}_API_KEY"),
        requests_per_second=float(os.environ.get(f"{prefix}_REQUESTS_PER_SECOND", "1.0")),
    )


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _optional_int(name: str) -> int | None:
    value = os.environ.get(name, "").strip()
    if not value:
        return None
    return int(value)


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())
