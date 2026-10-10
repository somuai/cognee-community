"""Unit test suite for Amazon Redshift connector (100% offline with mocks)."""

from unittest.mock import MagicMock

from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

from cognee_community_connector_redshift.config import RedshiftConfig
from cognee_community_connector_redshift.redshift import (
    REDSHIFT_SOURCE_NAME,
    RedshiftClient,
    _render_load_errors_markdown,
    _render_query_history_markdown,
    _render_table_markdown,
    redact_sensitive_sql,
    redshift_source,
)


def test_redshift_config_defaults():
    config = RedshiftConfig(database="analytics")
    assert config.transport == "data_api"
    assert config.database == "analytics"
    assert config.include_query_history is True
    assert config.include_load_errors is True
    assert config.write_disposition == "replace"


def test_redshift_secret_redaction():
    iam_sql = (
        "COPY table FROM 's3://bucket' IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole';"
    )
    scrubbed = redact_sensitive_sql(iam_sql)
    assert "[REDACTED_CREDENTIAL]" in scrubbed
    assert "arn:aws:iam" not in scrubbed

    key_sql = "COPY t FROM 's3://b' ACCESS_KEY_ID 'AKIAIOSFODNN7EXAMPLE';"
    scrubbed_key = redact_sensitive_sql(key_sql)
    assert "AKIAIOSFODNN7EXAMPLE" not in scrubbed_key


def test_execute_data_api_polling_and_pagination():
    mock_boto = MagicMock()
    # Step 1: execute_statement returns Id
    mock_boto.execute_statement.return_value = {"Id": "stmt-001"}
    # Step 2: describe_statement returns FINISHED
    mock_boto.describe_statement.return_value = {"Status": "FINISHED"}
    # Step 3: get_statement_result returns records with NextToken, then final page
    page1 = {
        "ColumnMetadata": [{"name": "id"}, {"name": "name"}],
        "Records": [[{"longValue": 1}, {"stringValue": "Alpha"}]],
        "NextToken": "page-2-token",
    }
    page2 = {
        "ColumnMetadata": [{"name": "id"}, {"name": "name"}],
        "Records": [[{"longValue": 2}, {"stringValue": "Beta"}]],
        "NextToken": None,
    }
    mock_boto.get_statement_result.side_effect = [page1, page2]

    config = RedshiftConfig(transport="data_api", database="test")
    client = RedshiftClient(config=config, boto3_client=mock_boto)

    records = client._execute_data_api("SELECT * FROM test;")
    assert len(records) == 2
    assert records[0] == {"id": 1, "name": "Alpha"}
    assert records[1] == {"id": 2, "name": "Beta"}
    assert mock_boto.get_statement_result.call_count == 2


def test_execute_db_api_cursor():
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("val",)]
    mock_cursor.fetchall.return_value = [(10, "data_a"), (20, "data_b")]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    config = RedshiftConfig(
        transport="db_api",
        host="redshift.example.com",
        database="analytics",
        user="dbuser",
        password="pwd",
    )
    client = RedshiftClient(config=config)
    client._db_conn = mock_conn
    results = client._execute_db_api("SELECT id, val FROM t;")

    assert len(results) == 2
    assert results[0] == {"id": 10, "val": "data_a"}
    assert results[1] == {"id": 20, "val": "data_b"}


def test_render_table_markdown():
    table_info = {
        "schema": "public",
        "table": "users",
        "diststyle": "KEY(user_id)",
        "sortkey1": "created_at",
        "tbl_rows": 1500000,
        "skew_rows": 1.05,
        "pct_used": 12.4,
    }
    cols = [
        {
            "column_name": "user_id",
            "data_type": "bigint",
            "is_nullable": "NO",
            "column_default": None,
        },
        {
            "column_name": "created_at",
            "data_type": "timestamp",
            "is_nullable": "NO",
            "column_default": "now()",
        },
    ]
    constraints = [{"constraint_name": "users_pk", "constraint_type": "PRIMARY KEY"}]

    md = _render_table_markdown(table_info, cols, constraints)
    assert "# Redshift Table: `public.users`" in md
    assert "KEY(user_id)" in md
    assert "created_at" in md
    assert "1500000" in md
    assert "`users_pk`" in md


def test_render_query_history_markdown():
    queries = [
        {
            "query_id": "q-101",
            "status": "SUCCESS",
            "elapsed_time": 42000,
            "start_time": "2026-10-10 12:00:00",
            "query_text": "SELECT COUNT(*) FROM public.orders;",
        }
    ]
    md = _render_query_history_markdown(queries)
    assert "# Redshift Operational Query Lineage" in md
    assert "`q-101`" in md
    assert "SELECT COUNT(*) FROM public.orders;" in md


def test_render_load_errors_markdown():
    errors = [
        {
            "starttime": "2026-10-10 12:05:00",
            "filename": "s3://raw-data/events.csv",
            "line_number": 42,
            "colname": "timestamp",
            "err_reason": "Invalid timestamp format",
        }
    ]
    md = _render_load_errors_markdown(errors)
    assert "# Redshift Recent COPY Load Errors" in md
    assert "events.csv" in md
    assert "Invalid timestamp format" in md


def test_redshift_source_full_sync():
    mock_client = MagicMock()
    mock_client.get_tables_info.return_value = [
        {
            "schema": "analytics",
            "table": "monthly_sales",
            "diststyle": "AUTO",
            "sortkey1": "sale_date",
        }
    ]
    mock_client.get_columns_info.return_value = [
        {
            "table_schema": "analytics",
            "table_name": "monthly_sales",
            "column_name": "sale_date",
            "data_type": "date",
        }
    ]
    mock_client.get_constraints_info.return_value = []
    mock_client.get_query_history.return_value = [
        {
            "query_id": "q-1",
            "status": "SUCCESS",
            "elapsed_time": 100,
            "start_time": "now",
            "query_text": "SELECT 1;",
        }
    ]
    mock_client.get_load_errors.return_value = []

    config = RedshiftConfig(database="analytics")
    source = redshift_source(config=config, client=mock_client)

    assert getattr(source, DOCUMENT_SOURCE_ATTR) == REDSHIFT_SOURCE_NAME

    items = list(next(iter(source.resources.values()))())
    assert len(items) == 2  # Table + query history

    table_item = items[0]
    assert table_item["entity_type"] == "table"
    assert table_item["id"] == "table:analytics.monthly_sales"

    query_item = items[1]
    assert query_item["entity_type"] == "query_history"


def test_redshift_source_filtering():
    mock_client = MagicMock()
    mock_client.get_tables_info.return_value = [
        {"schema": "keep_schema", "table": "keep_table"},
        {"schema": "skip_schema", "table": "skip_table"},
    ]
    mock_client.get_columns_info.return_value = []
    mock_client.get_constraints_info.return_value = []
    mock_client.get_query_history.return_value = []
    mock_client.get_load_errors.return_value = []

    config = RedshiftConfig(
        database="analytics",
        schemas=["keep_schema"],
        include_query_history=False,
        include_load_errors=False,
    )
    source = redshift_source(config=config, client=mock_client)

    items = list(next(iter(source.resources.values()))())
    assert len(items) == 1
    assert items[0]["schema"] == "keep_schema"
