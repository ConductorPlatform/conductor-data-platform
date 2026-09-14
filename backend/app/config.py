from __future__ import annotations

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="CONDUCTOR_",
    )

    # ── Database ──
    database_url: str = "postgresql+asyncpg://conductor:conductor@postgres:5432/conductor"

    # ── Redis ──
    redis_url: str = "redis://redis:6379/0"

    # ── Auth ──
    secret_key: str = "change-me-in-production"
    credentials_encryption_key: str | None = None
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    refresh_token_expire_days: int = 7
    redis_refresh_prefix: str = "refresh_token:"
    reauth_grant_ttl_seconds: int = 300
    reauth_rate_limit_attempts: int = 5
    reauth_rate_limit_window_seconds: int = 300
    trusted_proxy_cidrs: str = "127.0.0.0/8,::1/128"
    airflow_proxy_session_ttl_seconds: int = Field(default=300, gt=0)
    airflow_proxy_cookie_secure: bool = True

    # ── Durable lifecycle worker ──
    lifecycle_worker_poll_seconds: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    lifecycle_worker_lease_seconds: int = Field(default=30, gt=0)
    lifecycle_worker_heartbeat_seconds: float = Field(default=10.0, gt=0, allow_inf_nan=False)
    lifecycle_retry_base_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    lifecycle_retry_cap_seconds: float = Field(default=300.0, gt=0, allow_inf_nan=False)
    lifecycle_retry_jitter_seconds: float = Field(default=1.0, ge=0, allow_inf_nan=False)
    lifecycle_runtime_root: Path = Path("/var/lib/conductor/runtimes")
    # Global Docker volumes are mounted into trusted backend components. Project
    # stacks receive only an already-created project/generation subpath.
    lifecycle_runtime_secret_root: Path = Path("/var/lib/conductor/runtime-secrets")
    lifecycle_runtime_artifact_root: Path = Path("/var/lib/conductor/dbt-artifacts")
    lifecycle_runtime_ingress_network: str = "conductor-runtime-ingress"
    lifecycle_airflow_image: str = "conductor-airflow:latest"
    lifecycle_airflow_database_host: str = "host.docker.internal"
    lifecycle_airflow_database_port: int = Field(default=5432, ge=1, le=65535)
    lifecycle_maintenance_database_dsn: str | None = None
    lifecycle_warehouse_maintenance_dsn: str | None = None
    lifecycle_warehouse_host: str = "host.docker.internal"
    lifecycle_warehouse_port: int = Field(default=5433, ge=1, le=65535)
    lifecycle_airflow_ready_timeout_seconds: float = Field(default=120.0, gt=0, allow_inf_nan=False)
    lifecycle_airflow_ready_poll_seconds: float = Field(default=2.0, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_lifecycle_worker_timing(self) -> Settings:
        if self.lifecycle_worker_heartbeat_seconds >= self.lifecycle_worker_lease_seconds:
            raise ValueError("Lifecycle worker heartbeat must be shorter than its lease")
        if self.lifecycle_retry_base_seconds > self.lifecycle_retry_cap_seconds:
            raise ValueError("Lifecycle retry base must not exceed its cap")
        if self.lifecycle_airflow_ready_poll_seconds > self.lifecycle_airflow_ready_timeout_seconds:
            raise ValueError("Lifecycle readiness poll interval must not exceed its timeout")
        return self

    # ── Seed admin ──
    admin_email: str = "admin@conductor.local"
    admin_password: str = "admin"
    admin_name: str = "Admin"

    # ── Airflow (will be wired later) ──
    airflow_base_url: str = "http://airflow-api-server:8080"
    airflow_external_domain: str = "localhost"

    # ── Workspace (code-server) ──
    code_server_host: str = "http://code-server:8080"
    code_server_jwt_secret: str = "conductor-cs-jwt-secret-change-in-prod"


settings = Settings()
