"""
Databricks App: Victorian Site Consolidation Suitability Viewer

FastAPI backend serving spatial data from Lakebase (PostGIS) with
a Deck.gl/MapLibre GL frontend for interactive parcel visualization.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from db import get_pool, close_pool, fetch_one
from routes import parcels, candidates, stats, tiles
from models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown: manage DB pool."""
    await get_pool()
    yield
    await close_pool()


app = FastAPI(
    title="VIC Site Consolidation Viewer",
    description="Interactive viewer for Victorian land parcel consolidation suitability analysis",
    version="1.0.0",
    lifespan=lifespan,
)

# Register routers
app.include_router(parcels.router)
app.include_router(candidates.router)
app.include_router(stats.router)
app.include_router(tiles.router)


@app.get("/api/health", response_model=HealthResponse)
async def health():
    """Health check with basic stats."""
    row = await fetch_one("""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT suitability_tier) AS tiers
        FROM consolidation_candidates_sync
    """)
    return {
        "status": "healthy",
        "total_candidates": row["total"],
        "tier_count": row["tiers"],
    }


# Serve static frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def root():
    """Landing page with links to both viewer modes."""
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/tiles")
async def tiles_viewer():
    """Vector tile viewer (MVT) — fast pan/zoom, all 4.28M parcels."""
    return FileResponse(os.path.join(static_dir, "tiles.html"))


@app.get("/stream")
async def stream_viewer():
    """Streaming GeoJSON viewer (deck.gl) — loads all parcels in a single LGA."""
    return FileResponse(os.path.join(static_dir, "stream.html"))
