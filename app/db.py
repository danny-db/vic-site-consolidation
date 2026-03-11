"""Database connection for Databricks SQL Warehouse."""
import os
from databricks import sql as databricks_sql


def get_connection():
    """Create a new Databricks SQL connection."""
    host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
    return databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["SQL_WAREHOUSE_HTTP_PATH"],
        access_token=os.environ.get("DATABRICKS_TOKEN", ""),
    )


def get_table(name: str) -> str:
    """Get fully-qualified table name."""
    catalog = os.environ.get("UC_CATALOG", "danny_catalog")
    schema = os.environ.get("UC_SCHEMA", "dtp_hackathon")
    return f"{catalog}.{schema}.{name}"


def execute_query(query: str, parameters=None) -> list[dict]:
    """Execute a query and return results as list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, parameters=parameters)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]


def execute_query_one(query: str, parameters=None) -> dict | None:
    """Execute a query and return single result as dict."""
    results = execute_query(query, parameters)
    return results[0] if results else None
