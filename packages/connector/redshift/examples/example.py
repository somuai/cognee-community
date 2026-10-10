"""Example demonstrating Amazon Redshift data-source connector for Cognee.

This script demonstrates:
1. Configuring the Redshift connector via AWS Redshift Data API (Serverless or Provisioned).
2. Ingesting table physical layouts, schemas, PK/FK constraints, and operational query history.
3. Querying Cognee cognitive memory for table distribution styles and recent ETL load errors.
"""

import asyncio
import os

import cognee

from cognee_community_connector_redshift import RedshiftConfig, redshift_source

DATASET_NAME = "redshift_warehouse_memory"


async def main():
    # 1. Configure the Redshift source (Data API mode)
    config = RedshiftConfig(
        transport="data_api",
        workgroup_name=os.environ.get("REDSHIFT_WORKGROUP_NAME", "analytics-workgroup"),
        database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        schemas=["public", "analytics"],
        include_query_history=True,
        include_load_errors=True,
        write_disposition="replace",
    )

    source = redshift_source(config=config)

    # 2. Ingest Redshift warehouse topology into Cognee
    print("Syncing Redshift tables, distribution styles, and query history into Cognee...")
    await cognee.remember(source, dataset_name=DATASET_NAME)

    # 3. Query the knowledge graph for data warehouse layout and recent errors
    query = (
        "What is the distribution style and sort key for the monthly_orders table, "
        "and did any recent COPY operations encounter load errors?"
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
