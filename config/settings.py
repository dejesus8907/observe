"""Runtime settings and startup-facing configuration for NetObserv.

This module is the single source of truth for process-level configuration.
If this layer is sloppy, the rest of the platform becomes environment-dependent
trash: different workers boot with different defaults, startup behavior drifts,
and good instrumentation never actually gets wired in.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated NetObserv runtime configuration.

    Values are loaded from environment variables using the ``NETOBSERV_`` prefix.
    Validation is intentionally strict for startup-critical settings because weak
    startup configuration is how production systems end up booting into broken or
    unsafe states without anyone noticing.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NETOBSERV_",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Application identity
    # ------------------------------------------------------------------
    app_name: str = "NetObserv"
    app_version: str = "1.0.0"
    environment: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    log_format: str = "json"  # json or console

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    database_url: str = "sqlite+aiosqlite:///./netobserv.db"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    database_echo: bool = False

    # ------------------------------------------------------------------
    # API / server wiring
    # ------------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 1
    api_reload: bool = False
    api_docs_enabled: bool = True
    cors_enabled: bool = True
    cors_allow_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000", "http://127.0.0.1:3000"])
    cors_allow_credentials: bool = True
    cors_allow_methods: list[str] = Field(default_factory=lambda: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    cors_allow_headers: list[str] = Field(default_factory=lambda: ["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With"])

    # ------------------------------------------------------------------
    # Security posture
    # ------------------------------------------------------------------
    secret_key: str = "change-me-in-production-use-strong-random-key"
    access_token_expire_minutes: int = 30
    algorithm: str = "HS256"
    rbac_enabled: bool = False

    # ------------------------------------------------------------------
    # Discovery defaults
    # ------------------------------------------------------------------
    discovery_default_timeout: int = 30
    discovery_default_concurrency: int = 20
    discovery_max_retries: int = 3
    discovery_retry_backoff_base: float = 2.0
    discovery_read_only: bool = True

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    rate_limit_enabled: bool = True
    rate_limit_per_device: int = 5

    # ------------------------------------------------------------------
    # Observability wiring
    # ------------------------------------------------------------------
    metrics_enabled: bool = True
    tracing_enabled: bool = False

    # ------------------------------------------------------------------
    # Runtime execution subsystem
    # ------------------------------------------------------------------
    runtime_enabled: bool = True
    runtime_default_queue: str = "default"
    runtime_worker_id: str = "runtime-worker-1"
    runtime_lease_ttl_seconds: float = 30.0
    runtime_heartbeat_interval_seconds: float = 10.0
    runtime_max_jobs_per_run: int = 1
    runtime_worker_interval_seconds: float = 1.0
    runtime_scheduler_interval_seconds: float = 15.0
    runtime_idle_sleep_seconds: float = 1.0

    # ------------------------------------------------------------------
    # Streaming change detection subsystem
    # ------------------------------------------------------------------
    streaming_enabled: bool = True
    streaming_gnmi_enabled: bool = True
    streaming_ssh_poll_enabled: bool = True
    streaming_snmp_trap_enabled: bool = True
    streaming_syslog_enabled: bool = True
    streaming_event_bus_queue_size: int = 10000
    streaming_event_bus_dedup_window_seconds: float = 5.0
    streaming_subscriber_timeout_seconds: float = 2.0
    streaming_topology_delta_queue_size: int = 5000
    streaming_snmp_trap_host: str = "0.0.0.0"
    streaming_snmp_trap_port: int = 1162
    streaming_snmp_trap_community: str = "public"
    streaming_syslog_host: str = "0.0.0.0"
    streaming_syslog_udp_port: int = 1514
    streaming_syslog_tcp_port: int | None = 1514
    streaming_metrics_update_interval_seconds: float = 15.0
    streaming_service_heartbeat_interval_seconds: float = 30.0
    streaming_service_emit_health_log: bool = False
    tracing_service_name: str = "netobserv"
    tracing_endpoint: str | None = None

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------
    snapshot_retention_days: int = 90
    artifact_storage_path: str = "./artifacts"

    # ------------------------------------------------------------------
    # Topology / integrations
    # ------------------------------------------------------------------
    topology_min_confidence_threshold: float = 0.3
    netbox_url: str | None = None
    netbox_token: str | None = None
    netbox_verify_ssl: bool = True

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        allowed = {"development", "test", "staging", "production"}
        normalized = value.strip().lower()
        if normalized not in allowed:
            raise ValueError(f"environment must be one of {sorted(allowed)}")
        return normalized

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        normalized = value.strip().upper()
        if normalized not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return normalized

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"json", "console"}:
            raise ValueError("log_format must be 'json' or 'console'")
        return normalized

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        candidate = value.strip()
        if not candidate:
            raise ValueError("database_url must be set")
        return candidate

    @field_validator("api_host")
    @classmethod
    def validate_api_host(cls, value: str) -> str:
        candidate = value.strip()
        if not candidate:
            raise ValueError("api_host must be set")
        return candidate

    @field_validator("cors_allow_origins", "cors_allow_methods", "cors_allow_headers", mode="before")
    @classmethod
    def split_csv_values(cls, value: Any) -> Any:
        """Accept either JSON-like lists or comma-separated environment strings."""
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("cors_allow_origins")
    @classmethod
    def normalize_origins(cls, values: list[str]) -> list[str]:
        normalized = []
        for value in values:
            cleaned = value.strip()
            if not cleaned:
                continue
            normalized.append(cleaned.rstrip("/"))
        return sorted(set(normalized))

    @field_validator("cors_allow_methods", "cors_allow_headers")
    @classmethod
    def normalize_upper_lists(cls, values: list[str]) -> list[str]:
        normalized = []
        for value in values:
            cleaned = value.strip()
            if cleaned:
                normalized.append(cleaned.upper())
        return sorted(set(normalized))

    @field_validator(
        "database_pool_size",
        "database_max_overflow",
        "api_workers",
        "discovery_default_timeout",
        "discovery_default_concurrency",
        "discovery_max_retries",
        "rate_limit_per_device",
        "snapshot_retention_days",
        "access_token_expire_minutes",
    )
    @classmethod
    def validate_non_negative_ints(cls, value: int) -> int:
        if value < 0:
            raise ValueError("value must be >= 0")
        return value

    @field_validator("api_port")
    @classmethod
    def validate_api_port(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError("api_port must be between 1 and 65535")
        return value

    @field_validator("discovery_retry_backoff_base", "topology_min_confidence_threshold")
    @classmethod
    def validate_non_negative_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("value must be >= 0")
        return value

    @field_validator("runtime_default_queue", "runtime_worker_id")
    @classmethod
    def _validate_runtime_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("runtime queue/worker values may not be blank")
        return value

    @field_validator("streaming_snmp_trap_host", "streaming_syslog_host")
    @classmethod
    def _validate_streaming_hosts(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("streaming host values may not be blank")
        return value

    @model_validator(mode="after")
    def validate_startup_profile(self) -> Settings:
        """Catch contradictory or unsafe startup combinations early."""
        if self.api_reload and self.environment == "production":
            raise ValueError("api_reload must be disabled in production")
        if self.api_reload and self.api_workers != 1:
            raise ValueError("api_reload requires api_workers=1")
        if self.environment == "production" and self.cors_enabled:
            if not self.cors_allow_origins:
                raise ValueError("production CORS requires explicit allowed origins")
            if "*" in self.cors_allow_origins:
                raise ValueError("wildcard CORS is forbidden in production")
        if self.environment == "production" and self.secret_key.startswith("change-me"):
            raise ValueError("secret_key must be overridden in production")
        if self.tracing_enabled and not self.tracing_service_name.strip():
            raise ValueError("tracing_service_name must be set when tracing is enabled")
        return self

    @property
    def docs_url(self) -> str | None:
        return "/api/docs" if self.api_docs_enabled else None

    @property
    def redoc_url(self) -> str | None:
        return "/api/redoc" if self.api_docs_enabled else None

    @property
    def openapi_url(self) -> str | None:
        return "/api/openapi.json" if self.api_docs_enabled else None

    @property
    def artifact_storage_dir(self) -> Path:
        return Path(self.artifact_storage_path).expanduser().resolve()

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_sqlite(self) -> bool:
        return "sqlite" in self.database_url.lower()


@lru_cache
def get_settings() -> Settings:
    """Return a cached settings instance for the current process."""
    return Settings()


def reset_settings_cache() -> None:
    """Clear cached settings.

    Tests and one-shot CLI commands need this to force re-evaluation after
    environment changes. Without it, startup wiring quietly keeps using stale
    configuration and people waste hours debugging the wrong process state.
    """
    get_settings.cache_clear()
