"""Unit test suite for Prometheus connector (100% offline with mocks)."""

from unittest.mock import MagicMock, patch

import pytest
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

from cognee_community_connector_prometheus.config import PrometheusConfig
from cognee_community_connector_prometheus.prometheus import (
    PROMETHEUS_SOURCE_NAME,
    PrometheusClient,
    _compute_rule_id,
    _render_promql_summary_markdown,
    _render_rule_markdown,
    _render_targets_markdown,
    prometheus_source,
    redact_dictionary,
    redact_sensitive_value,
)


def test_config_defaults_and_validation():
    config = PrometheusConfig(base_url="https://prometheus.internal:9090/")
    assert config.base_url == "https://prometheus.internal:9090"
    assert config.rule_type == "all"
    assert config.include_targets is False
    assert config.write_disposition == "replace"

    with pytest.raises(ValueError, match="base_url must start with"):
        PrometheusConfig(base_url="ftp://invalid-url")


def test_config_basic_auth_resolution():
    config = PrometheusConfig(
        base_url="http://localhost:9090",
        username="admin",
        password="secretpassword",
    )
    assert config.basic_auth == ("admin", "secretpassword")


def test_secret_redaction():
    assert redact_sensitive_value("api_key", "secret123") == "[REDACTED]"
    assert redact_sensitive_value("bearer_token", "abc") == "[REDACTED]"
    assert redact_sensitive_value("authorization", "Bearer eyJhbGciOi...") == "[REDACTED]"
    assert redact_sensitive_value("metric_name", "up") == "up"

    data = {
        "user_secret": "hidden",
        "scrape_url": "http://user:password@target/metrics",
        "nested": {"token_value": "secret456", "safe": "normal"},
    }
    redacted = redact_dictionary(data)
    assert redacted["user_secret"] == "[REDACTED]"
    assert redacted["nested"]["token_value"] == "[REDACTED]"
    assert redacted["nested"]["safe"] == "normal"


def test_deterministic_rule_id():
    id1 = _compute_rule_id(
        "/etc/rules.yml", "node-alerts", "alert", "NodeDown", {"severity": "critical"}
    )
    id2 = _compute_rule_id(
        "/etc/rules.yml", "node-alerts", "alert", "NodeDown", {"severity": "critical"}
    )
    id3 = _compute_rule_id(
        "/etc/rules.yml", "node-alerts", "alert", "NodeDown", {"severity": "warning"}
    )

    assert id1 == id2
    assert id1 != id3


def test_client_headers_and_auth():
    client = PrometheusClient(
        base_url="http://prom:9090",
        bearer_token="my-token",
        extra_headers={"X-Scope-OrgID": "tenant-a"},
    )
    headers = client._get_headers()
    assert headers["Authorization"] == "Bearer my-token"
    assert headers["X-Scope-OrgID"] == "tenant-a"
    assert headers["Accept"] == "application/json"


@patch("httpx.Client")
def test_client_retry_logic(mock_client_cls):
    import httpx

    mock_client = MagicMock()
    # First 2 attempts raise TimeoutException, 3rd succeeds
    mock_client.get.side_effect = [
        httpx.TimeoutException("Connection timed out"),
        httpx.TimeoutException("Connection timed out"),
        MagicMock(status_code=200, json=lambda: {"status": "success", "data": {"result": []}}),
    ]
    mock_client_cls.return_value.__enter__.return_value = mock_client

    client = PrometheusClient(base_url="http://prom:9090", max_retries=3, retry_backoff_factor=0.01)
    result = client.get("api/v1/query", params={"query": "up"})

    assert result["status"] == "success"
    assert mock_client.get.call_count == 3


def test_render_rule_markdown():
    rule = {
        "name": "HighMemoryUsage",
        "type": "alerting",
        "query": "node_memory_usage_ratio > 0.9",
        "duration": 300,
        "health": "ok",
        "labels": {"severity": "critical", "secret_auth": "should_hide"},
        "annotations": {"summary": "Node memory is above 90%"},
    }
    md = _render_rule_markdown("/etc/rules.yaml", "node_alerts", rule, "http://localhost:9090")

    assert "# Prometheus Rule: HighMemoryUsage" in md
    assert "node_memory_usage_ratio > 0.9" in md
    assert "`severity=critical`" in md
    assert "[REDACTED]" in md
    assert "Node memory is above 90%" in md


def test_render_targets_markdown():
    targets = [
        {
            "scrapePool": "node_exporter",
            "scrapeUrl": "http://10.0.0.1:9100/metrics",
            "health": "up",
            "lastScrape": "2026-10-10T12:00:00Z",
            "labels": {"job": "node_exporter", "instance": "worker-1"},
        }
    ]
    md = _render_targets_markdown("node_exporter", targets)
    assert "# Prometheus Scrape Pool: node_exporter" in md
    assert "http://10.0.0.1:9100/metrics" in md
    assert "worker-1" in md


def test_render_promql_summary_markdown():
    res = {
        "resultType": "vector",
        "result": [
            {
                "metric": {"__name__": "up", "instance": "db-primary:9100"},
                "value": [1728561600, "1"],
            }
        ],
    }
    md = _render_promql_summary_markdown("up", res)
    assert "# Prometheus Metric Summary: up" in md
    assert "db-primary:9100" in md
    assert "| `1` |" in md


def test_prometheus_source_full_sync():
    mock_client = MagicMock()
    mock_client.base_url = "http://localhost:9090"
    mock_client.list_rules.return_value = [
        {
            "name": "infra_rules",
            "file": "/etc/prom/infra.yml",
            "rules": [
                {
                    "name": "HighCPU",
                    "type": "alerting",
                    "query": "cpu_utilization > 80",
                    "duration": 60,
                    "health": "ok",
                    "labels": {"severity": "warning"},
                    "annotations": {"description": "CPU > 80%"},
                }
            ],
        }
    ]
    mock_client.list_targets.return_value = {
        "activeTargets": [
            {
                "scrapePool": "api_servers",
                "scrapeUrl": "http://api:8080/metrics",
                "health": "up",
                "labels": {"app": "api"},
            }
        ]
    }
    mock_client.query_instant.return_value = {
        "resultType": "vector",
        "result": [{"metric": {"job": "api"}, "value": [1000, "1"]}],
    }

    config = PrometheusConfig(
        base_url="http://localhost:9090",
        include_targets=True,
        promql_queries=["up{job='api'}"],
    )

    source = prometheus_source(config=config, client=mock_client)

    assert getattr(source, DOCUMENT_SOURCE_ATTR) == PROMETHEUS_SOURCE_NAME

    # Extract resource items
    resource_func = next(iter(source.resources.values()))
    items = list(resource_func())

    assert len(items) == 3

    # Check rule item
    rule_item = items[0]
    assert rule_item["entity_type"] == "rule"
    assert "HighCPU" in rule_item["title"]
    assert "rule:" in rule_item["id"]

    # Check targets item
    target_item = items[1]
    assert target_item["entity_type"] == "scrape_pool"
    assert target_item["pool_name"] == "api_servers"

    # Check PromQL summary item
    metric_item = items[2]
    assert metric_item["entity_type"] == "metric_summary"
    assert metric_item["query"] == "up{job='api'}"


def test_prometheus_source_filters():
    mock_client = MagicMock()
    mock_client.base_url = "http://localhost:9090"
    mock_client.list_rules.return_value = [
        {
            "name": "group_keep",
            "file": "/etc/prom/keep.yml",
            "rules": [{"name": "R1", "type": "alert", "labels": {}}],
        },
        {
            "name": "group_skip",
            "file": "/etc/prom/skip.yml",
            "rules": [{"name": "R2", "type": "alert", "labels": {}}],
        },
    ]

    config = PrometheusConfig(
        base_url="http://localhost:9090",
        file_filter=["/etc/prom/keep.yml"],
    )
    source = prometheus_source(config=config, client=mock_client)
    items = list(next(iter(source.resources.values()))())

    assert len(items) == 1
    assert items[0]["file"] == "/etc/prom/keep.yml"
