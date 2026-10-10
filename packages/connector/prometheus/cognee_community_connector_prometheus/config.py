"""Configuration models for the Prometheus Cognee data-source connector."""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class PrometheusConfig(BaseModel):
    """Pydantic configuration for connecting to a Prometheus HTTP API v1 server.

    Supports stock unauthenticated Prometheus servers, Bearer token authentication,
    HTTP Basic authentication, custom proxy headers, path-prefixed base URLs
    (Thanos, Mimir, VictoriaMetrics), opt-in metadata/target/alert/PromQL
    enrichment, and watermark cursor tracking.
    """

    base_url: str = Field(
        default_factory=lambda: os.environ.get("PROMETHEUS_URL", "http://localhost:9090"),
        description="Base URL of the Prometheus server (supports path prefixes).",
    )
    bearer_token: str | None = Field(
        default_factory=lambda: os.environ.get("PROMETHEUS_BEARER_TOKEN"),
        description="Optional Bearer token for Authorization header.",
    )
    basic_auth: tuple[str, str] | None = Field(
        default=None,
        description="Optional (username, password) tuple for HTTP Basic authentication.",
    )
    username: str | None = Field(
        default_factory=lambda: os.environ.get("PROMETHEUS_USERNAME"),
        description="Optional username for HTTP Basic authentication.",
    )
    password: str | None = Field(
        default_factory=lambda: os.environ.get("PROMETHEUS_PASSWORD"),
        description="Optional password for HTTP Basic authentication.",
    )
    extra_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Additional HTTP headers (e.g., X-Scope-OrgID for Mimir/Cortex).",
    )
    rule_type: Literal["alert", "record", "all"] = Field(
        default="all",
        description="Filter rules by type: 'alert', 'record', or 'all'.",
    )
    file_filter: list[str] | None = Field(
        default=None,
        description="Optional allowlist of rule file paths to ingest.",
    )
    group_filter: list[str] | None = Field(
        default=None,
        description="Optional allowlist of rule group names to ingest.",
    )
    include_metadata: bool = Field(
        default=False,
        description="Whether to ingest metric help/type/unit from /api/v1/metadata.",
    )
    include_targets: bool = Field(
        default=False,
        description="Whether to ingest active scrape pool topology from /api/v1/targets.",
    )
    include_alerts: bool = Field(
        default=False,
        description="Whether to ingest active firing/pending alerts from /api/v1/alerts.",
    )
    promql_queries: list[str] = Field(
        default_factory=list,
        description=(
            "Allowlist of PromQL queries to evaluate via /api/v1/query and /api/v1/query_range."
        ),
    )
    target_scrape_pools: list[str] | None = Field(
        default=None,
        description="Optional allowlist of scrape pool names when include_targets is enabled.",
    )
    lookback_seconds: int = Field(
        default=3600,
        ge=60,
        description="Lookback window in seconds for PromQL range summary queries.",
    )
    step_seconds: int = Field(
        default=300,
        ge=1,
        description="Query resolution step width in seconds for /api/v1/query_range.",
    )
    last_evaluation_timestamp: float | None = Field(
        default=None,
        description="Optional watermark cursor (Unix epoch seconds) for incremental tracking.",
    )
    max_retries: int = Field(
        default=5,
        ge=1,
        description="Maximum HTTP retry attempts on transient 429/5xx or network errors.",
    )
    retry_backoff_factor: float = Field(
        default=0.5,
        gt=0.0,
        description="Base backoff multiplier in seconds for exponential retry delays.",
    )
    timeout_seconds: float = Field(
        default=15.0,
        gt=0.0,
        description="HTTP request timeout in seconds.",
    )
    batch_size: int = Field(
        default=50,
        ge=1,
        description="Number of documents passed per batch into cognee.add(data_per_batch=...).",
    )
    dataset_name: str = Field(
        default="prometheus_memory",
        min_length=1,
        description="Target Cognee dataset name.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Whether to verify TLS certificates on HTTPS connections.",
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description="dlt write disposition ('replace' enables full-snapshot orphan cleanup).",
    )

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        cleaned = value.strip().rstrip("/")
        if not cleaned.startswith(("http://", "https://")):
            raise ValueError("base_url must start with 'http://' or 'https://'")
        return cleaned

    @model_validator(mode="after")
    def _resolve_basic_auth(self) -> PrometheusConfig:
        if self.basic_auth is None and self.username and self.password is not None:
            self.basic_auth = (self.username, self.password)
        return self
