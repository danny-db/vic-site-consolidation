"""
Databricks App: Victorian Site Consolidation Suitability Viewer

FastAPI backend serving spatial data from Databricks SQL Warehouse with
a Deck.gl/MapLibre GL frontend for interactive parcel visualization.
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from .routes import parcels, candidates, stats
from .models import HealthResponse
from .db import execute_query_one, get_table


app = FastAPI(
    title="VIC Site Consolidation Viewer",
    description="Interactive viewer for Victorian land parcel consolidation suitability analysis",
    version="1.0.0",
)

# Register routers
app.include_router(parcels.router)
app.include_router(candidates.router)
app.include_router(stats.router)


@app.get("/api/health", response_model=HealthResponse)
def health():
    """Health check with basic stats."""
    table = get_table("consolidation_candidates")
    row = execute_query_one(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT suitability_tier) AS tiers
        FROM {table}
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
def root():
    """Serve the frontend."""
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "VIC Site Consolidation API", "docs": "/docs"}
