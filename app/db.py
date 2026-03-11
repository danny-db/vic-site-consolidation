"""Database connection pool for Lakebase (PostGIS)."""
import os
import asyncpg


_pool = None


async def get_pool() -> asyncpg.Pool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=os.environ["LAKEBASE_HOST"],
            port=int(os.environ.get("LAKEBASE_PORT", "5432")),
            database=os.environ["LAKEBASE_DATABASE"],
            user=os.environ["LAKEBASE_USER"],
            password=os.environ["LAKEBASE_PASSWORD"],
            ssl="require",
            min_size=2,
            max_size=10,
        )
    return _pool


async def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
