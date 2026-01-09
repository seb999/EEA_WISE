"""
System health check and status endpoints.

This module contains endpoints for checking service health and status.
"""

from fastapi import APIRouter
from typing import Dict, Any

from ..ogc_features import OGCConformance, OGCCollections

# Create router
router = APIRouter()

# Services will be set by main app
data_service = None
ogc_collections: OGCCollections = None


def init_router(service, collections: OGCCollections):
    """Initialize router with data service and collections."""
    global data_service, ogc_collections
    data_service = service
    ogc_collections = collections


@router.get("/healthCheck", tags=["System"])
async def service_status() -> Dict[str, Any]:
    """Get status of data service and API features."""

    service_info = data_service.get_service_info() if data_service else {}

    return {
        "service_status": {
            "data_service_available": data_service is not None,
            "active_data_service": service_info.get('active_service', 'none'),
            "configured_mode": service_info.get('configured_mode', 'unknown')
        },
        "api_version": "4.0.0",
        "ogc_compliance": {
            "ogc_api_features": True,
            "conformance_classes": len(OGCConformance.get_conformance_declaration()["conformsTo"]),
            "collections": ogc_collections.list_collection_ids() if ogc_collections else []
        },
        "features": {
            "data_connection": data_service is not None,
            "ogc_geojson_support": True,
            "ogc_collections": True,
            "bbox_filtering": True,
            "pagination": True,
            "coordinate_enrichment": "SQL JOIN (optimized)",
            "switchable_backends": True,
            "service_info": service_info
        },
    }
