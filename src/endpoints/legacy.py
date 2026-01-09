"""
Legacy OGC endpoints for backward compatibility.

This module contains Phase 1 OGC endpoints that are maintained for backward
compatibility with existing clients.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
from datetime import datetime

from ..utils import validate_bbox, flatten_dremio_data
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
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of features to return"),
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

        # Build query for spatial locations
        base_query = '''
        SELECT
            thematicIdIdentifier,
            thematicIdIdentifierScheme,
            lat as latitude,
            lon as longitude,
            monitoringSiteIdentifier,
            monitoringSiteName,
            countryCode
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
        WHERE lat IS NOT NULL AND lon IS NOT NULL
        '''

        # Add country filter
        if country_code:
            base_query += f" AND UPPER(countryCode) = UPPER('{country_code}')"

        # Add bounding box filter if provided
        if bbox:
            min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
            base_query += f" AND lon >= {min_lon} AND lon <= {max_lon}"
            base_query += f" AND lat >= {min_lat} AND lat <= {max_lat}"

        base_query += f" LIMIT {limit}"

        # Execute query
        result = data_service.execute_query(base_query)
        flattened_data = flatten_dremio_data(result)

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
