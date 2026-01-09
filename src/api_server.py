"""
EEA WISE Data API Server.

Main FastAPI application that provides OGC API - Features compliant endpoints
for accessing water quality data from the EEA WISE_SOE database.
"""

from fastapi import FastAPI
import uvicorn
from .dremio_service import DremioApiService
from .ogc_features import OGCCollections
from .endpoints import ogc_core, timeseries, legacy, metadata, system
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EEA WISE Data API",
    description="API service to retrieve water quality disaggregated data from the European Environment Agency (EEA) WISE_SOE database using Dremio data lake. Supports OGC API - Features compliance with GeoJSON output.",
    version="4.0.0",
    openapi_tags=[
        {
            "name": "System",
            "description": "Health check and system information endpoints"
        },
        {
            "name": "OGC API - Features Core",
            "description": "OGC API - Features Part 1 Core endpoints for standards-compliant geospatial data access"
        },
        {
            "name": "OGC Collections",
            "description": "Query items from OGC collections (monitoring sites, measurements, disaggregated data)"
        },
        {
            "name": "Time-Series",
            "description": "Specialized endpoints for temporal water quality data with aggregation capabilities"
        },
        {
            "name": "Metadata",
            "description": "Discovery endpoints for available parameters and monitoring sites"
        },
        {
            "name": "Legacy OGC",
            "description": "Phase 1 OGC endpoints (maintained for backward compatibility)"
        }
    ]
)

# Initialize Dremio Data service
try:
    data_service = DremioApiService()
    service_info = data_service.get_service_info()
    logger.info(f"✓ Data service initialized successfully: {service_info['active_service']} ({service_info['service_class']})")
except Exception as e:
    logger.error(f"✗ Failed to initialize data service: {e}")
    data_service = None

# Initialize OGC collections
ogc_collections = OGCCollections()
logger.info(f"✓ Initialized {len(ogc_collections.list_collection_ids())} OGC collections")

# Initialize endpoint routers with required services
ogc_core.init_router(ogc_collections, data_service)
timeseries.init_router(data_service)
legacy.init_router(data_service)
metadata.init_router(data_service)
system.init_router(data_service, ogc_collections)

# Include routers in the main app
app.include_router(ogc_core.router)
app.include_router(timeseries.router)
app.include_router(legacy.router)
app.include_router(metadata.router)
app.include_router(system.router)

logger.info("✓ All endpoint routers registered successfully")


def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    start_server()
