from functools import lru_cache
from typing import Annotated

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )

    app_name: str = "deft-indexer"
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8080)

    database_url: str = Field(
        default="postgresql+psycopg://deft:deft@postgres:5432/deft"
    )
    redis_url: str = Field(default="redis://redis:6379/0")

    kafka_bootstrap_servers: str = Field(default="redpanda:9092")
    kafka_topic: str = Field(default="deft.events")

    rpc_url: Annotated[str, AnyHttpUrl | str] = Field(default="http://localhost:8545")
    chunk_size: int = Field(
        default=2500, description="DEPRECATED: Use initial_chunk_size instead"
    )
    batch_concurrency: int = Field(default=4, ge=1)

    rpc_requests_per_second: float = Field(
        default=10.0,
        gt=0,
        description="Maximum RPC requests per second to avoid rate limiting",
    )
    rpc_rate_limit_retry_delay: int = Field(
        default=60,
        ge=1,
        description="Seconds to wait before retrying after 429 rate limit error",
    )

    # Finality and reorg protection
    finality_blocks: int = Field(
        default=64,
        ge=1,
        description="Reorg safety window; only process blocks older than this",
    )
    poll_overlap_blocks: int = Field(
        default=24, ge=0, description="Block overlap on subsequent polls to heal gaps"
    )

    # Adaptive chunk control
    initial_chunk_size: int = Field(
        default=1000, ge=1, description="Starting chunk size for adaptive controller"
    )
    min_chunk_size: int = Field(
        default=100, ge=1, description="Minimum allowed chunk size"
    )
    max_chunk_size: int = Field(
        default=10000, ge=1, description="Maximum allowed chunk size"
    )

    # API security
    trust_proxy: bool = Field(
        default=False,
        description="Trust X-Forwarded-For header for client IP (only enable behind trusted proxy)",
    )

    metrics_port: int = Field(default=9000)


@lru_cache
def get_settings() -> Settings:
    return Settings()
