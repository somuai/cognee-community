"""DLT source for Prometheus observability metadata (rules, targets, alerts, metrics, PromQL).

Fetches alerting and recording rules, active scrape targets, firing/pending alerts,
metric metadata, and PromQL instant/range query summaries from a Prometheus HTTP API v1 server,
then yields them as a dlt document resource for Cognee's ingestion pipeline.

Declares ``cognee_document_source = "prometheus"``, so ``resolve_dlt_sources`` routes
each entity through Cognee's cognify entity-extraction and knowledge graph completion pipeline.

Implements a full-snapshot replacement model: ``write_disposition="replace"`` ensures each sync
overwrites staging with the exact set of alerting rules, active targets, and metric definitions
currently visible in the Prometheus instance. If a rule or target is decommissioned or deleted,
it drops out of subsequent syncs and is cleanly purged via ``orphan_cleanup``.
"""

import hashlib
import json
import re
import time
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

from .config import PrometheusConfig

logger = get_logger("prometheus_connector")

PROMETHEUS_TABLE_NAME = "prometheus_entities"
PROMETHEUS_SOURCE_NAME = "prometheus"

_MAX_RETRIES = 5
_RETRY_BACKOFF_FACTOR = 0.5
_SECRET_PATTERN = re.compile(r"(token|secret|password|key|credential|auth)", re.IGNORECASE)


def redact_sensitive_value(key: str, val: Any) -> Any:
    """Redact sensitive values based on key naming."""
    if isinstance(key, str) and _SECRET_PATTERN.search(key):
        return "[REDACTED]"
    if isinstance(val, str) and re.match(
        r"^(Bearer\s+[A-Za-z0-9\-._~+/]+=*|basic\s+[A-Za-z0-9+/=]+)", val, re.IGNORECASE
    ):
        return "[REDACTED]"
    return val


def redact_dictionary(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively redact sensitive key-value pairs in a dictionary."""
    redacted = {}
    for k, v in data.items():
        if isinstance(v, dict):
            redacted[k] = redact_dictionary(v)
        elif isinstance(v, list):
            redacted[k] = [
                redact_dictionary(elem)
                if isinstance(elem, dict)
                else redact_sensitive_value(k, elem)
                for elem in v
            ]
        else:
            redacted[k] = redact_sensitive_value(k, v)
    return redacted


class PrometheusClient:
    """HTTP client for querying Prometheus HTTP API v1 endpoints with retry backoff."""

    def __init__(
        self,
        base_url: str = "http://localhost:9090",
        bearer_token: str | None = None,
        basic_auth: tuple[str, str] | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float = 15.0,
        verify_ssl: bool = True,
        max_retries: int = _MAX_RETRIES,
        retry_backoff_factor: float = _RETRY_BACKOFF_FACTOR,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.bearer_token = bearer_token
        self.basic_auth = basic_auth
        self.extra_headers = extra_headers or {}
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor

    def _get_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": "cognee-prometheus-connector/0.1.0",
        }
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        headers.update(self.extra_headers)
        return headers

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Perform GET request with exponential backoff on transient errors."""
        import httpx

        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = self._get_headers()
        auth = self.basic_auth if self.basic_auth else None

        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout, verify=self.verify_ssl) as client:
                    resp = client.get(url, headers=headers, params=params, auth=auth)
                    if resp.status_code == 404:
                        logger.warning("Prometheus endpoint %s not found (404)", url)
                        return {}
                    resp.raise_for_status()
                    payload = resp.json()
                    if payload.get("status") == "error":
                        logger.warning("Prometheus API error: %s", payload.get("error"))
                    return payload
            except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                resp_code = getattr(getattr(exc, "response", None), "status_code", 0)
                is_transient = isinstance(
                    exc, httpx.ConnectError | httpx.TimeoutException
                ) or resp_code in (429, 500, 502, 503, 504)
                if is_transient and attempt < self.max_retries:
                    backoff = self.retry_backoff_factor * (2 ** (attempt - 1))
                    logger.warning(
                        "Transient error contacting %s (%s). Retrying in %.2fs (attempt %d/%d)...",
                        url,
                        exc,
                        backoff,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(backoff)
                else:
                    logger.error("Failed contacting Prometheus %s: %s", url, exc)
                    raise

        return {}

    def list_rules(self, rule_type: str = "all") -> list[dict[str, Any]]:
        """Retrieve alerting and recording rule groups."""
        params = {}
        if rule_type in ("alert", "record"):
            params["type"] = rule_type
        data = self.get("api/v1/rules", params=params)
        groups = data.get("data", {}).get("groups", [])
        return groups

    def list_targets(self) -> dict[str, Any]:
        """Retrieve active and dropped scrape targets."""
        data = self.get("api/v1/targets")
        return data.get("data", {})

    def list_alerts(self) -> list[dict[str, Any]]:
        """Retrieve active firing and pending alerts."""
        data = self.get("api/v1/alerts")
        return data.get("data", {}).get("alerts", [])

    def get_metadata(self, metric: str | None = None) -> dict[str, list[dict[str, Any]]]:
        """Retrieve metric metadata (help, type, unit)."""
        params = {}
        if metric:
            params["metric"] = metric
        data = self.get("api/v1/metadata", params=params)
        return data.get("data", {})

    def query_instant(self, query: str) -> dict[str, Any]:
        """Evaluate an instant PromQL query."""
        data = self.get("api/v1/query", params={"query": query})
        return data.get("data", {})

    def query_range(
        self, query: str, start: float, end: float, step_seconds: int = 300
    ) -> dict[str, Any]:
        """Evaluate a range PromQL query."""
        params = {
            "query": query,
            "start": str(start),
            "end": str(end),
            "step": f"{step_seconds}s",
        }
        data = self.get("api/v1/query_range", params=params)
        return data.get("data", {})


def _compute_rule_id(
    file_path: str, group_name: str, rule_type: str, rule_name: str, labels: dict[str, Any]
) -> str:
    """Generate a deterministic composite rule ID."""
    canonical_labels = json.dumps(labels, sort_keys=True)
    raw = f"{file_path}:{group_name}:{rule_type}:{rule_name}:{canonical_labels}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _render_rule_markdown(
    file_path: str, group_name: str, rule: dict[str, Any], base_url: str
) -> str:
    """Render a clean Markdown document for an alerting or recording rule."""
    rule_type = rule.get("type", "unknown")
    name = rule.get("name", "unnamed_rule")
    query = rule.get("query", "-")
    duration = rule.get("duration", 0)
    health = rule.get("health", "unknown")
    last_error = rule.get("lastError") or "None"
    evaluation_time = rule.get("evaluationTime", "-")

    labels = redact_dictionary(rule.get("labels", {}))
    annotations = redact_dictionary(rule.get("annotations", {}))

    labels_str = ", ".join(f"`{k}={v}`" for k, v in labels.items()) if labels else "None"
    annotations_str = (
        "\n".join(f"- **{k}**: {v}" for k, v in annotations.items()) if annotations else "None"
    )

    return f"""# Prometheus Rule: {name} ({rule_type})

- **Rule Name**: `{name}`
- **Type**: `{rule_type}`
- **Group**: `{group_name}`
- **File**: `{file_path}`
- **Health**: `{health}`
- **Duration**: `{duration}s`
- **Last Evaluation Time**: `{evaluation_time}`
- **Last Evaluation Error**: {last_error}

## Expression (PromQL)
```promql
{query}
```

## Labels
{labels_str}

## Annotations
{annotations_str}
""".strip()


def _render_targets_markdown(scrape_pool: str, targets: list[dict[str, Any]]) -> str:
    """Render scrape pool target topology into Markdown."""
    lines = [
        f"# Prometheus Scrape Pool: {scrape_pool}",
        "",
        f"- **Scrape Pool**: `{scrape_pool}`",
        f"- **Active Targets Count**: {len(targets)}",
        "",
        "## Target Instances",
        "| Discovered Labels | Scrape URL | Health | Last Scrape | Last Error |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for t in targets:
        labels = redact_dictionary(t.get("labels", {}))
        scrape_url = t.get("scrapeUrl", "-")
        health = t.get("health", "unknown")
        last_scrape = t.get("lastScrape", "-")
        last_error = t.get("lastError") or "None"
        lbl_str = ", ".join(f"`{k}={v}`" for k, v in sorted(labels.items()))
        lines.append(f"| {lbl_str} | `{scrape_url}` | `{health}` | {last_scrape} | {last_error} |")

    return "\n".join(lines).strip()


def _render_promql_summary_markdown(query: str, result_data: dict[str, Any]) -> str:
    """Render PromQL evaluation summary into Markdown."""
    result_type = result_data.get("resultType", "unknown")
    results = result_data.get("result", [])

    lines = [
        f"# Prometheus Metric Summary: {query}",
        "",
        f"- **Query**: `{query}`",
        f"- **Result Type**: `{result_type}`",
        f"- **Series Count**: {len(results)}",
        "",
        "## Evaluated Series",
        "| Metric Labels | Value |",
        "| :--- | :--- |",
    ]
    for r in results[:50]:  # Cap at top 50 series to prevent document bloat
        metric = redact_dictionary(r.get("metric", {}))
        metric_str = ", ".join(f"`{k}={v}`" for k, v in sorted(metric.items())) or "Scalar"
        val = r.get("value", ["-", "-"])[1] if "value" in r else str(r.get("values", "-"))
        lines.append(f"| {metric_str} | `{val}` |")

    return "\n".join(lines).strip()


def prometheus_source(
    config: PrometheusConfig | None = None,
    client: PrometheusClient | None = None,
):
    """Create a dlt source yielding Prometheus rules, targets, and metric summaries.

    Entities are formatted as structured Markdown documents.

    Args:
        config: Configuration instance.
        client: Optional injected PrometheusClient (or mock) for testing.
    """
    import dlt

    cfg = config or PrometheusConfig()
    resolved_client = client or PrometheusClient(
        base_url=cfg.base_url,
        bearer_token=cfg.bearer_token,
        basic_auth=cfg.basic_auth,
        extra_headers=cfg.extra_headers,
        timeout=cfg.timeout_seconds,
        verify_ssl=cfg.verify_ssl,
        max_retries=cfg.max_retries,
        retry_backoff_factor=cfg.retry_backoff_factor,
    )

    @dlt.source(name=PROMETHEUS_SOURCE_NAME)
    def _prometheus():
        @dlt.resource(
            name=PROMETHEUS_TABLE_NAME,
            write_disposition=cfg.write_disposition,
        )
        def prometheus_entities() -> Iterator[dict[str, Any]]:
            # 1. Sync Rules (Alerting and Recording)
            try:
                groups = resolved_client.list_rules(rule_type=cfg.rule_type)
            except Exception as exc:
                logger.error("Failed to list Prometheus rules: %s", exc)
                raise

            for group in groups:
                group_name = group.get("name", "unknown")
                file_path = group.get("file", "unknown")

                if cfg.file_filter and file_path not in cfg.file_filter:
                    continue
                if cfg.group_filter and group_name not in cfg.group_filter:
                    continue

                for rule in group.get("rules", []):
                    rule_type = rule.get("type", "unknown")
                    rule_name = rule.get("name", "unnamed")
                    labels = rule.get("labels", {})

                    rule_id = _compute_rule_id(file_path, group_name, rule_type, rule_name, labels)
                    content = _render_rule_markdown(
                        file_path, group_name, rule, resolved_client.base_url
                    )

                    yield {
                        "id": f"rule:{rule_id}",
                        "url": f"{resolved_client.base_url}/api/v1/rules#{rule_id}",
                        "title": f"Prometheus Rule: {rule_name} ({rule_type})",
                        "content": content,
                        "entity_type": "rule",
                        "group": group_name,
                        "file": file_path,
                    }

            # 2. Sync Targets Topology if enabled
            if cfg.include_targets:
                try:
                    target_data = resolved_client.list_targets()
                    active_targets = target_data.get("activeTargets", [])
                    pools: dict[str, list[dict[str, Any]]] = {}
                    for t in active_targets:
                        pool = t.get("scrapePool", "default")
                        if cfg.target_scrape_pools and pool not in cfg.target_scrape_pools:
                            continue
                        pools.setdefault(pool, []).append(t)

                    for pool, targets in pools.items():
                        pool_id = hashlib.sha256(pool.encode("utf-8")).hexdigest()[:12]
                        content = _render_targets_markdown(pool, targets)
                        yield {
                            "id": f"target_pool:{pool_id}",
                            "url": f"{resolved_client.base_url}/api/v1/targets#{pool}",
                            "title": f"Prometheus Scrape Pool: {pool}",
                            "content": content,
                            "entity_type": "scrape_pool",
                            "pool_name": pool,
                        }
                except Exception as exc:
                    logger.warning("Could not sync Prometheus scrape targets: %s", exc)

            # 3. Sync PromQL query summaries if specified
            if cfg.promql_queries:
                for query in cfg.promql_queries:
                    try:
                        res = resolved_client.query_instant(query)
                        q_id = hashlib.sha256(query.encode("utf-8")).hexdigest()[:12]
                        content = _render_promql_summary_markdown(query, res)
                        yield {
                            "id": f"promql:{q_id}",
                            "url": f"{resolved_client.base_url}/api/v1/query?query={query}",
                            "title": f"Prometheus Metric Summary: {query}",
                            "content": content,
                            "entity_type": "metric_summary",
                            "query": query,
                        }
                    except Exception as exc:
                        logger.warning("Could not evaluate PromQL query %s: %s", query, exc)

        return prometheus_entities

    source = _prometheus()
    setattr(source, DOCUMENT_SOURCE_ATTR, PROMETHEUS_SOURCE_NAME)
    return source
