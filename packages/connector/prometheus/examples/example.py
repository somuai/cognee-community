"""Example demonstrating Prometheus data-source connector for Cognee.

This script demonstrates:
1. Configuring the Prometheus source to ingest alerting rules, recording rules,
   scrape targets, and PromQL metric summaries.
2. Ingesting Prometheus observability metadata into Cognee cognitive memory.
3. Querying the knowledge graph for rule alert thresholds and failing scrape targets.
"""

import asyncio
import os

import cognee

from cognee_community_connector_prometheus import PrometheusConfig, prometheus_source

DATASET_NAME = "prometheus_observability_memory"


async def main():
    # 1. Configure the Prometheus source
    config = PrometheusConfig(
        base_url=os.environ.get("PROMETHEUS_URL", "http://localhost:9090"),
        bearer_token=os.environ.get("PROMETHEUS_BEARER_TOKEN"),
        rule_type="all",
        include_targets=True,
        promql_queries=[
            "up",
            "rate(http_requests_total[5m])",
        ],
        write_disposition="replace",
    )

    source = prometheus_source(config=config)

    # 2. Ingest Prometheus observability into Cognee
    print("Syncing Prometheus rules, scrape targets, and PromQL summaries into Cognee...")
    await cognee.remember(source, dataset_name=DATASET_NAME)

    # 3. Query the knowledge graph for firing rule conditions and scrape health
    query = (
        "Which alerting rules monitor HTTP error rates or target availability, "
        "and what are their threshold expressions and severities?"
    )
    print(f"\nQuerying knowledge graph: '{query}'\n")

    search_results = await cognee.search(
        query_text=query,
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )

    print("Agent Search Results:")
    print(search_results)


if __name__ == "__main__":
    asyncio.run(main())
