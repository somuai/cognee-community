"""Configuration models for the Amazon Redshift Cognee data-source connector."""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field


class RedshiftConfig(BaseModel):
    """Configuration for Amazon Redshift data-source connector.

    Supports dual transport modes:
    - 'data_api': Serverless or Provisioned clusters via AWS boto3 redshift-data HTTPS client.
    - 'db_api': Direct relational connection via redshift-connector / PostgreSQL wire protocol.
    """

    transport: Literal["data_api", "db_api"] = Field(
        default="data_api",
        description="Transport mode: 'data_api' (boto3) or 'db_api' (redshift-connector).",
    )

    # Redshift Data API fields
    workgroup_name: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_WORKGROUP_NAME"),
        description="Redshift Serverless workgroup name.",
    )
    cluster_identifier: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER"),
        description="Redshift Provisioned cluster identifier.",
    )
    database: str = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_DATABASE", "dev"),
        description="Target Redshift database name.",
    )
    secret_arn: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_SECRET_ARN"),
        description="AWS Secrets Manager ARN for Data API credentials.",
    )
    db_user: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_DB_USER"),
        description="Database username for IAM authentication in Data API.",
    )
    region_name: str = Field(
        default_factory=lambda: os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        description="AWS Region name.",
    )

    # Redshift DB-API fields
    host: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_HOST"),
        description="Redshift cluster endpoint host.",
    )
    port: int = Field(
        default=5439,
        ge=1,
        le=65535,
        description="Redshift port (defaults to 5439).",
    )
    user: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_USER"),
        description="Database user for direct connection.",
    )
    password: str | None = Field(
        default_factory=lambda: os.environ.get("REDSHIFT_PASSWORD"),
        description="Database password for direct connection.",
    )

    # Filtering & feature toggles
    schemas: list[str] | None = Field(
        default=None,
        description="Optional allowlist of schema names to ingest (e.g. ['public', 'analytics']).",
    )
    tables: list[str] | None = Field(
        default=None,
        description="Optional allowlist of table names to ingest.",
    )
    include_query_history: bool = Field(
        default=True,
        description="Whether to ingest recent operational query history from SYS_QUERY_HISTORY.",
    )
    include_load_errors: bool = Field(
        default=True,
        description="Whether to ingest recent COPY load errors from STL_LOAD_ERRORS.",
    )
    query_history_limit: int = Field(
        default=25,
        ge=1,
        description="Maximum recent query history executions to capture.",
    )
    max_retries: int = Field(
        default=5,
        ge=1,
        description="Maximum retry attempts on transient throttling or network errors.",
    )
    retry_backoff_factor: float = Field(
        default=0.5,
        gt=0.0,
        description="Base multiplier for exponential backoff delays.",
    )
    batch_size: int = Field(
        default=50,
        ge=1,
        description="Batch size for document emission.",
    )
    dataset_name: str = Field(
        default="redshift_memory",
        min_length=1,
        description="Target Cognee dataset name.",
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description="dlt write disposition ('replace' enables orphan cleanup on dropped tables).",
    )
