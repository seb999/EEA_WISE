"""
Legacy OGC endpoints for backward compatibility.

This module contains Phase 1 OGC endpoints that are maintained for backward
compatibility with existing clients.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
from datetime import datetime

from ..utils import validate_bbox
from ..geojson_formatter import GeoJSONFormatter

# Create router
router = APIRouter()

# Data service will be set by main app
data_service = None


def init_router(service):
    """Initialize router with data service."""
    global data_service
    data_service = service


@router.get("/ogc/spatial-locations", tags=["Legacy OGC"])
async def get_ogc_spatial_locations(
    country_code: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'FR')"),
    limit: int = Query(1000, ge=1, le=50000, description="Maximum number of features to return"),
    bbox: Optional[str] = Query(None, description="Bounding box filter: minLon,minLat,maxLon,maxLat")
) -> Dict[str, Any]:
    """
    OGC-compliant endpoint to retrieve monitoring site locations as GeoJSON.

    This endpoint returns spatial locations from the Waterbase_S_WISE_SpatialObject_DerivedData table
    in OGC-compliant GeoJSON FeatureCollection format.

    **Note:** This is a legacy endpoint maintained for backward compatibility.
    New clients should use `/collections/monitoring-sites/items` instead.

    Args:
        country_code: Optional country code filter (e.g., 'DE', 'FR')
        limit: Maximum number of features to return (1-10000)
        bbox: Optional bounding box filter (minLon,minLat,maxLon,maxLat)

    Returns:
        GeoJSON FeatureCollection with monitoring site locations
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_MonitoringSites"
        fields = [
            "thematicIdIdentifier",
            "thematicIdIdentifierScheme",
            "lat",
            "lon",
            "monitoringSiteIdentifier",
            "monitoringSiteName",
            "countryCode"
        ]

        filters = []

        if country_code:
            filters.append({"fieldName": "countryCode", "condition": "=", "values": [country_code.upper()], "concat": "AND"})

        if bbox:
            min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
            filters.append({"fieldName": "lon", "condition": ">=", "values": [min_lon], "concat": "AND"})
            filters.append({"fieldName": "lon", "condition": "<=", "values": [max_lon], "concat": "AND"})
            filters.append({"fieldName": "lat", "condition": ">=", "values": [min_lat], "concat": "AND"})
            filters.append({"fieldName": "lat", "condition": "<=", "values": [max_lat], "concat": "AND"})

        result = data_service.execute_view_query(VIEW_PATH, fields, filters, limit=limit)
        flattened_data = result if isinstance(result, list) else []

        # Rename lat/lon to latitude/longitude for GeoJSON formatter
        for item in flattened_data:
            if "lat" in item:
                item["latitude"] = item.pop("lat")
            if "lon" in item:
                item["longitude"] = item.pop("lon")

        # Convert to GeoJSON
        geojson_response = GeoJSONFormatter.format_spatial_locations(
            flattened_data,
            country_code
        )

        # Add OGC-compliant links
        base_url = "/ogc/spatial-locations"
        geojson_response["links"] = [
            {
                "href": base_url,
                "rel": "self",
                "type": "application/geo+json",
                "title": "This document"
            }
        ]

        # Add timestamp
        geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

        return geojson_response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch spatial locations: {str(e)}"
        )
