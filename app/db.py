"""Database connection pool for Lakebase (PostGIS)."""
import os
import ssl
import logging
import asyncpg

logger = logging.getLogger(__name__)

_pool = None
_refresh_lock = None


def _get_lakebase_credentials() -> dict:
    """Get Lakebase connection credentials from env vars."""
    pg_host = os.getenv("PGHOST") or os.getenv("LAKEBASE_HOST", "localhost")
    pg_port = os.getenv("PGPORT") or os.getenv("LAKEBASE_PORT", "5432")
    pg_database = os.getenv("PGDATABASE") or os.getenv("LAKEBASE_DATABASE", "vic_consolidation_db")
    pg_user = os.getenv("PGUSER") or os.getenv("LAKEBASE_USER", "vic_consolidation_app")
    pg_password = os.getenv("LAKEBASE_PASSWORD", "")

    logger.info(f"Connecting as {pg_user} to {pg_host}:{pg_port}/{pg_database}")

    return {
        "host": pg_host,
        "port": int(pg_port),
        "database": pg_database,
        "user": pg_user,
        "password": pg_password,
    }


def _get_ssl_context():
    """Create SSL context for Lakebase connection."""
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    return ssl_ctx


async def get_pool() -> asyncpg.Pool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None:
        schema = os.getenv("LAKEBASE_SCHEMA", "dtp_hackathon")
        creds = _get_lakebase_credentials()
        _pool = await asyncpg.create_pool(
            host=creds["host"],
            port=creds["port"],
            database=creds["database"],
            user=creds["user"],
            password=creds["password"],
            min_size=2,
            max_size=10,
            server_settings={"search_path": f"{schema}, public"},
            ssl=_get_ssl_context(),
        )
        logger.info("Database pool created successfully")
    return _pool


async def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def _refresh_pool() -> None:
    """Close stale pool and create a new one."""
    import asyncio
    global _refresh_lock
    if _refresh_lock is None:
        _refresh_lock = asyncio.Lock()

    async with _refresh_lock:
        global _pool
        if _pool:
            try:
                await _pool.close()
            except Exception:
                pass
            _pool = None
        await get_pool()
        logger.info("Connection pool refreshed")


async def fetch_all(query: str, *args) -> list[dict]:
    """Execute a query and return every row as a list of dicts."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired, refreshing pool...")
        await _refresh_pool()
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]


async def fetch_one(query: str, *args) -> dict | None:
    """Execute a query and return a single row as a dict."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
    except asyncpg.exceptions.InvalidPasswordError:
        logger.warning("Token expired, refreshing pool...")
        await _refresh_pool()
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None
