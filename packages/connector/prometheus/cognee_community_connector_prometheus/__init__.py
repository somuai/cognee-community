"""Prometheus connector package for Cognee."""

from .config import PrometheusConfig
from .prometheus import (
    DOCUMENT_SOURCE_ATTR,
    PROMETHEUS_SOURCE_NAME,
    PROMETHEUS_TABLE_NAME,
    PrometheusClient,
    prometheus_source,
)

__all__ = [
    "DOCUMENT_SOURCE_ATTR",
    "PROMETHEUS_SOURCE_NAME",
    "PROMETHEUS_TABLE_NAME",
    "PrometheusClient",
    "PrometheusConfig",
    "prometheus_source",
]
