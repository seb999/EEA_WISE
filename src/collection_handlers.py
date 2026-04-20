"""
OGC API - Features collection item handlers.

This module contains handler functions for retrieving items from OGC collections.
Each handler implements the logic for a specific collection type (monitoring-sites,
latest-measurements, disaggregated-data).
"""

from typing import Dict, Any, Optional
from fastapi import Request, HTTPException
from datetime import datetime

from .utils import validate_bbox, flatten_dremio_data
from .geojson_formatter import GeoJSONFormatter
from .ogc_features import OGCLinks


async def get_monitoring_sites_items(
    data_service,
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """
    Handler for monitoring-sites collection items.

    Args:
        data_service: DremioApiService instance
        request: FastAPI request object
        limit: Maximum number of items to return
        offset: Number of items to skip
        bbox: Optional bounding box filter
        country_code: Optional country code filter

    Returns:
        GeoJSON FeatureCollection with monitoring site locations
    """
    # View path for spatial data
    VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_MonitoringSites"

    # Fields to select
    fields = [
        "thematicIdIdentifier",
        "thematicIdIdentifierScheme",
        "monitoringSiteIdentifier",
        "monitoringSiteName",
        "countryCode",
        "lat",
        "lon"
    ]

    # Build filters
    filters = []

    if country_code:
        filters.append({"fieldName": "countryCode", "condition": "=", "values": [country_code], "concat": "AND"})

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        filters.append({"fieldName": "lon", "condition": ">=", "values": [str(min_lon)], "concat": "AND"})
        filters.append({"fieldName": "lon", "condition": "<=", "values": [str(max_lon)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": ">=", "values": [str(min_lat)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": "<=", "values": [str(max_lat)], "concat": "AND"})

    # Get data with pagination — middleware returns a flat list of dicts
    result = data_service.execute_view_query(VIEW_PATH, fields, filters, limit=limit, offset=offset)
    data = result if isinstance(result, list) else flatten_dremio_data(result)

    # Use returned row count (exact count not available via view query)
    total_count = len(data)

    # Rename lat/lon to latitude/longitude for GeoJSON formatter
    for item in data:
        item['latitude'] = item.pop('lat', None)
        item['longitude'] = item.pop('lon', None)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_spatial_locations(data, country_code)

    # Build base URL and add pagination links
    base_url = str(request.base_url).rstrip('')
    collection_url = f"{base_url}/collections/monitoring-sites/items"

    extra_params = {}
    if country_code:
        extra_params['country_code'] = country_code
    if bbox:
        extra_params['bbox'] = bbox

    geojson_response["links"] = OGCLinks.create_pagination_links(
        collection_url, offset, limit, total_count, extra_params
    )

    # Add collection link (required by OGC)
    geojson_response["links"].append({
        "href": f"{base_url}/collections/monitoring-sites",
        "rel": "collection",
        "type": "application/json",
        "title": "The monitoring-sites collection"
    })

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response


async def get_latest_measurements_items(
    data_service,
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """
    Handler for latest-measurements collection items.

    Args:
        data_service: DremioApiService instance
        request: FastAPI request object
        limit: Maximum number of items to return
        offset: Number of items to skip
        bbox: Optional bounding box filter
        country_code: Optional country code filter

    Returns:
        GeoJSON FeatureCollection with latest measurements
    """
    # View path — JOIN + date filter already baked into the Dremio view
    VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_LatestMeasurements"

    # Fields to select
    fields = [
        "monitoringSiteIdentifier",
        "monitoringSiteIdentifierScheme",
        "observedPropertyDeterminandCode",
        "observedPropertyDeterminandLabel",
        "phenomenonTimeSamplingDate",
        "resultObservedValue",
        "resultUom",
        "countryCode",
        "lat",
        "lon",
        "monitoringSiteName"
    ]

    # Build filters
    filters = []

    if country_code:
        filters.append({"fieldName": "countryCode", "condition": "=", "values": [country_code], "concat": "AND"})

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        filters.append({"fieldName": "lon", "condition": ">=", "values": [str(min_lon)], "concat": "AND"})
        filters.append({"fieldName": "lon", "condition": "<=", "values": [str(max_lon)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": ">=", "values": [str(min_lat)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": "<=", "values": [str(max_lat)], "concat": "AND"})

    # Get data with pagination — middleware returns a flat list of dicts
    result = data_service.execute_view_query(VIEW_PATH, fields, filters, limit=limit, offset=offset)
    data = result if isinstance(result, list) else flatten_dremio_data(result)

    total_count = len(data)

    # Rename lat/lon/monitoringSiteName to match coordinate format expected by GeoJSON formatter
    for item in data:
        item['coordinates'] = {
            'latitude': item.pop('lat', None),
            'longitude': item.pop('lon', None)
        }
        item['coordinate_siteName'] = item.pop('monitoringSiteName', None)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_measurements_with_location(data)

    # Build base URL and add pagination links
    base_url = str(request.base_url).rstrip('/')
    collection_url = f"{base_url}/collections/latest-measurements/items"

    extra_params = {}
    if country_code:
        extra_params['country_code'] = country_code
    if bbox:
        extra_params['bbox'] = bbox

    geojson_response["links"] = OGCLinks.create_pagination_links(
        collection_url, offset, limit, total_count, extra_params
    )

    # Add collection link (required by OGC)
    geojson_response["links"].append({
        "href": f"{base_url}/collections/latest-measurements",
        "rel": "collection",
        "type": "application/json",
        "title": "The latest-measurements collection"
    })

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response


async def get_disaggregated_data_items(
    data_service,
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """
    Handler for disaggregated-data collection items.

    Args:
        data_service: DremioApiService instance
        request: FastAPI request object
        limit: Maximum number of items to return
        offset: Number of items to skip
        bbox: Optional bounding box filter
        country_code: Optional country code filter

    Returns:
        GeoJSON FeatureCollection with disaggregated water quality data
    """
    # View path — JOIN baked into the Dremio view (no WHERE in view)
    VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_DisaggregatedData"

    # Fields to select
    fields = [
        "monitoringSiteIdentifier",
        "monitoringSiteIdentifierScheme",
        "observedPropertyDeterminandCode",
        "observedPropertyDeterminandLabel",
        "phenomenonTimeSamplingDate",
        "resultObservedValue",
        "resultUom",
        "countryCode",
        "parameterWaterBodyCategory",
        "lat",
        "lon",
        "monitoringSiteName"
    ]

    # Build filters
    filters = []

    if country_code:
        filters.append({"fieldName": "countryCode", "condition": "=", "values": [country_code], "concat": "AND"})

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        filters.append({"fieldName": "lon", "condition": ">=", "values": [str(min_lon)], "concat": "AND"})
        filters.append({"fieldName": "lon", "condition": "<=", "values": [str(max_lon)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": ">=", "values": [str(min_lat)], "concat": "AND"})
        filters.append({"fieldName": "lat", "condition": "<=", "values": [str(max_lat)], "concat": "AND"})

    # Get data with pagination — middleware returns a flat list of dicts
    result = data_service.execute_view_query(VIEW_PATH, fields, filters, limit=limit, offset=offset)
    data = result if isinstance(result, list) else flatten_dremio_data(result)

    total_count = len(data)

    # Rename lat/lon/monitoringSiteName to match coordinate format expected by GeoJSON formatter
    for item in data:
        item['coordinates'] = {
            'latitude': item.pop('lat', None),
            'longitude': item.pop('lon', None)
        }
        item['coordinate_siteName'] = item.pop('monitoringSiteName', None)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_measurements_with_location(data)

    # Build base URL and add pagination links
    base_url = str(request.base_url).rstrip('/')
    collection_url = f"{base_url}/collections/disaggregated-data/items"

    extra_params = {}
    if country_code:
        extra_params['country_code'] = country_code
    if bbox:
        extra_params['bbox'] = bbox

    geojson_response["links"] = OGCLinks.create_pagination_links(
        collection_url, offset, limit, total_count, extra_params
    )

    # Add collection link (required by OGC)
    geojson_response["links"].append({
        "href": f"{base_url}/collections/disaggregated-data",
        "rel": "collection",
        "type": "application/json",
        "title": "The disaggregated-data collection"
    })

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response
