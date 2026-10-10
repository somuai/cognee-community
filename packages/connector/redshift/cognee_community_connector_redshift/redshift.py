"""DLT source for Amazon Redshift metadata and query lineage.

Fetches data warehouse table layouts from SVV_TABLE_INFO (diststyle, sortkey, skew),
column definitions from SVV_COLUMNS, PK/FK constraints, and operational query history
from SYS_QUERY_HISTORY / STL_LOAD_ERRORS into Cognee cognitive memory graphs.

Declares ``cognee_document_source = "redshift"``, so ``resolve_dlt_sources`` routes
each entity through Cognee's cognify entity-extraction and knowledge graph completion pipeline.

Implements a full-snapshot replacement model: ``write_disposition="replace"`` ensures each sync
overwrites staging with the exact set of tables currently present in Redshift. If a table
is dropped or migrated, it drops out of subsequent syncs and is cleanly
purged via ``orphan_cleanup``.
"""

import re
import time
from collections.abc import Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

from .config import RedshiftConfig

logger = get_logger("redshift_connector")

REDSHIFT_TABLE_NAME = "redshift_entities"
REDSHIFT_SOURCE_NAME = "redshift"

_MAX_RETRIES = 5
_RETRY_BACKOFF_FACTOR = 0.5
_SECRET_PATTERN = re.compile(
    r"(arn:aws:iam::\d+:role/[\w+=,.@-]+"
    r"|AKIA[0-9A-Z]{16}"
    r"|[A-Za-z0-9+/]{40}"
    r"|password\s*=\s*'[^']*'"
    r"|credentials\s*['\"][^'\"]*['\"])",
    re.IGNORECASE,
)


def redact_sensitive_sql(sql_text: str) -> str:
    """Scrub IAM roles, AWS keys, passwords, and COPY credentials from SQL queries."""
    if not sql_text:
        return ""
    scrubbed = _SECRET_PATTERN.sub("[REDACTED_CREDENTIAL]", sql_text)
    return scrubbed


class RedshiftClient:
    """Client for executing queries against Redshift via Data API or direct DB-API."""

    def __init__(self, config: RedshiftConfig, boto3_client: Any | None = None) -> None:
        self.config = config
        self._data_client = boto3_client
        self._db_conn = None

    def _get_data_client(self) -> Any:
        if self._data_client is None:
            import boto3

            self._data_client = boto3.client("redshift-data", region_name=self.config.region_name)
        return self._data_client

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute a query with retry backoff and return records as dicts."""
        if self.config.transport == "data_api":
            return self._execute_data_api(sql)
        return self._execute_db_api(sql)

    def _execute_data_api(self, sql: str) -> list[dict[str, Any]]:
        """Execute query via AWS Redshift Data API."""
        client = self._get_data_client()
        params: dict[str, Any] = {"Database": self.config.database, "Sql": sql}

        if self.config.workgroup_name:
            params["WorkgroupName"] = self.config.workgroup_name
        elif self.config.cluster_identifier:
            params["ClusterIdentifier"] = self.config.cluster_identifier
            if self.config.secret_arn:
                params["SecretArn"] = self.config.secret_arn
            elif self.config.db_user:
                params["DbUser"] = self.config.db_user

        for attempt in range(1, self.config.max_retries + 1):
            try:
                resp = client.execute_statement(**params)
                statement_id = resp["Id"]
                break
            except Exception as exc:
                if attempt < self.config.max_retries:
                    backoff = self.config.retry_backoff_factor * (2 ** (attempt - 1))
                    logger.warning("Throttled executing Redshift statement: %s. Retrying...", exc)
                    time.sleep(backoff)
                else:
                    raise

        # Poll for completion
        while True:
            desc = client.describe_statement(Id=statement_id)
            status = desc.get("Status")
            if status == "FINISHED":
                break
            if status in ("FAILED", "ABORTED"):
                error_msg = desc.get("Error", "Unknown execution error")
                logger.error("Redshift statement %s %s: %s", statement_id, status, error_msg)
                raise RuntimeError(f"Redshift statement execution {status}: {error_msg}")
            time.sleep(0.5)

        # Retrieve paginated records
        records: list[dict[str, Any]] = []
        next_token = None
        while True:
            get_params: dict[str, Any] = {"Id": statement_id}
            if next_token:
                get_params["NextToken"] = next_token
            res = client.get_statement_result(**get_params)
            columns = [col["name"] for col in res.get("ColumnMetadata", [])]
            for row in res.get("Records", []):
                record = {}
                for idx, col_name in enumerate(columns):
                    val_dict = row[idx]
                    val = next(iter(val_dict.values())) if val_dict else None
                    record[col_name] = val
                records.append(record)
            next_token = res.get("NextToken")
            if not next_token:
                break

        return records

    def _execute_db_api(self, sql: str) -> list[dict[str, Any]]:
        """Execute query via direct relational connection."""
        if self._db_conn is None:
            import redshift_connector

            self._db_conn = redshift_connector.connect(
                host=self.config.host,
                database=self.config.database,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
            )
        cursor = self._db_conn.cursor()
        cursor.execute(sql)
        desc = cursor.description
        columns = [col[0] for col in desc] if desc else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def get_tables_info(self) -> list[dict[str, Any]]:
        """Query SVV_TABLE_INFO for physical layout metrics."""
        sql = """
        SELECT "schema", "table", diststyle, sortkey1, skew_rows, unsorted, pct_used, tbl_rows
        FROM svv_table_info
        ORDER BY "schema", "table";
        """
        return self.execute(sql)

    def get_columns_info(self) -> list[dict[str, Any]]:
        """Query SVV_COLUMNS for table column specifications."""
        sql = """
        SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
        FROM svv_columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position;
        """
        return self.execute(sql)

    def get_constraints_info(self) -> list[dict[str, Any]]:
        """Query constraints for primary key and foreign key relationships."""
        sql = """
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            con.conname AS constraint_name,
            con.contype AS constraint_type
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
        """
        try:
            return self.execute(sql)
        except Exception:
            return []

    def get_query_history(self, limit: int = 25) -> list[dict[str, Any]]:
        """Query recent SQL statements from SYS_QUERY_HISTORY."""
        sql = f"""
        SELECT query_id, user_id, start_time, end_time, elapsed_time, status, query_text
        FROM sys_query_history
        ORDER BY start_time DESC
        LIMIT {limit};
        """
        try:
            return self.execute(sql)
        except Exception:
            return []

    def get_load_errors(self, limit: int = 25) -> list[dict[str, Any]]:
        """Query recent COPY command errors from STL_LOAD_ERRORS."""
        sql = f"""
        SELECT starttime, filename, line_number, colname, err_reason, raw_line
        FROM stl_load_errors
        ORDER BY starttime DESC
        LIMIT {limit};
        """
        try:
            return self.execute(sql)
        except Exception:
            return []


def _render_table_markdown(
    table: dict[str, Any], columns: list[dict[str, Any]], constraints: list[dict[str, Any]]
) -> str:
    """Render a Redshift table physical layout and column schema into Markdown."""
    schema = table.get("schema", "public")
    tbl_name = table.get("table", "unknown")
    diststyle = table.get("diststyle", "AUTO")
    sortkey = table.get("sortkey1", "NONE")
    tbl_rows = table.get("tbl_rows", "-")
    skew = table.get("skew_rows", "-")
    pct_used = table.get("pct_used", "-")

    lines = [
        f"# Redshift Table: `{schema}.{tbl_name}`",
        "",
        f"- **Schema**: `{schema}`",
        f"- **Table**: `{tbl_name}`",
        f"- **Distribution Style**: `{diststyle}`",
        f"- **Primary Sort Key**: `{sortkey}`",
        f"- **Approximate Rows**: {tbl_rows}",
        f"- **Row Skew**: {skew}",
        f"- **Disk Used**: {pct_used}%",
        f"- **Column Count**: {len(columns)}",
        "",
        "## Columns",
        "| Column Name | Type | Nullable | Default |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for c in columns:
        c_name = c.get("column_name", "-")
        c_type = c.get("data_type", "-")
        c_null = c.get("is_nullable", "YES")
        c_def = c.get("column_default") or "-"
        lines.append(f"| `{c_name}` | `{c_type}` | {c_null} | {c_def} |")

    if constraints:
        lines.extend(["", "## Constraints", "| Constraint Name | Type |", "| :--- | :--- |"])
        for con in constraints:
            lines.append(f"| `{con.get('constraint_name')}` | `{con.get('constraint_type')}` |")

    return "\n".join(lines).strip()


def _render_query_history_markdown(queries: list[dict[str, Any]]) -> str:
    """Render recent operational query execution history into Markdown."""
    lines = [
        "# Redshift Operational Query Lineage",
        "",
        f"- **Total Captured Queries**: {len(queries)}",
        "",
        "## Recent Executions",
        "| Query ID | Status | Elapsed (us) | Start Time | Query Text |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for q in queries:
        q_id = q.get("query_id", "-")
        status = q.get("status", "-")
        elapsed = q.get("elapsed_time", "-")
        start = q.get("start_time", "-")
        raw_text = q.get("query_text", "-")
        scrubbed_text = redact_sensitive_sql(str(raw_text)).replace("\n", " ")[:150]
        lines.append(f"| `{q_id}` | `{status}` | {elapsed} | {start} | `{scrubbed_text}` |")

    return "\n".join(lines).strip()


def _render_load_errors_markdown(errors: list[dict[str, Any]]) -> str:
    """Render recent COPY load errors into Markdown."""
    lines = [
        "# Redshift Recent COPY Load Errors",
        "",
        f"- **Total Captured Errors**: {len(errors)}",
        "",
        "## Load Errors",
        "| Timestamp | File | Line | Column | Reason |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for e in errors:
        ts = e.get("starttime", "-")
        fn = e.get("filename", "-")
        ln = e.get("line_number", "-")
        col = e.get("colname", "-")
        reason = e.get("err_reason", "-")
        lines.append(f"| {ts} | `{fn}` | {ln} | `{col}` | {reason} |")

    return "\n".join(lines).strip()


def redshift_source(
    config: RedshiftConfig | None = None,
    client: RedshiftClient | None = None,
):
    """Create a dlt source yielding Redshift schema and query lineage as Markdown documents.

    Args:
        config: Configuration instance.
        client: Optional injected RedshiftClient (or mock) for testing.
    """
    import dlt

    cfg = config or RedshiftConfig()
    resolved_client = client or RedshiftClient(config=cfg)

    @dlt.source(name=REDSHIFT_SOURCE_NAME)
    def _redshift():
        @dlt.resource(
            name=REDSHIFT_TABLE_NAME,
            write_disposition=cfg.write_disposition,
        )
        def redshift_entities() -> Iterator[dict[str, Any]]:
            # 1. Ingest table layouts & schemas
            try:
                tables = resolved_client.get_tables_info()
                columns = resolved_client.get_columns_info()
                constraints = resolved_client.get_constraints_info()
            except Exception as exc:
                logger.error("Failed to query Redshift catalog: %s", exc)
                raise

            cols_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for col in columns:
                key = (col.get("table_schema", ""), col.get("table_name", ""))
                cols_by_table.setdefault(key, []).append(col)

            cons_by_table: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for con in constraints:
                key = (con.get("table_schema", ""), con.get("table_name", ""))
                cons_by_table.setdefault(key, []).append(con)

            for t in tables:
                schema = t.get("schema", "public")
                tbl_name = t.get("table", "unknown")

                if cfg.schemas and schema not in cfg.schemas:
                    continue
                if cfg.tables and tbl_name not in cfg.tables:
                    continue

                table_cols = cols_by_table.get((schema, tbl_name), [])
                table_cons = cons_by_table.get((schema, tbl_name), [])
                content = _render_table_markdown(t, table_cols, table_cons)

                yield {
                    "id": f"table:{schema}.{tbl_name}",
                    "url": f"redshift://{cfg.database}/{schema}/{tbl_name}",
                    "title": f"Redshift Table: {schema}.{tbl_name}",
                    "content": content,
                    "entity_type": "table",
                    "schema": schema,
                    "table": tbl_name,
                }

            # 2. Ingest operational query history if enabled
            if cfg.include_query_history:
                queries = resolved_client.get_query_history(limit=cfg.query_history_limit)
                if queries:
                    q_content = _render_query_history_markdown(queries)
                    yield {
                        "id": "query_history:recent",
                        "url": f"redshift://{cfg.database}/sys_query_history",
                        "title": "Redshift Operational Query History",
                        "content": q_content,
                        "entity_type": "query_history",
                    }

            # 3. Ingest recent load errors if enabled
            if cfg.include_load_errors:
                errors = resolved_client.get_load_errors(limit=cfg.query_history_limit)
                if errors:
                    err_content = _render_load_errors_markdown(errors)
                    yield {
                        "id": "load_errors:recent",
                        "url": f"redshift://{cfg.database}/stl_load_errors",
                        "title": "Redshift Recent COPY Load Errors",
                        "content": err_content,
                        "entity_type": "load_errors",
                    }

        return redshift_entities

    source = _redshift()
    setattr(source, DOCUMENT_SOURCE_ATTR, REDSHIFT_SOURCE_NAME)
    return source
