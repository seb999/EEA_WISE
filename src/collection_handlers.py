"""
OGC API - Features collection item handlers.

This module contains handler functions for retrieving items from OGC collections.
Each handler implements the logic for a specific collection type (monitoring-sites,
latest-measurements, disaggregated-data).
"""

from typing import Dict, Any, Optional
from fastapi import Request, HTTPException
from datetime import datetime

from .utils import validate_bbox, flatten_dremio_data, format_optimized_coordinates
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
    query = f"""
    SELECT
        thematicIdIdentifier,
        thematicIdIdentifierScheme,
        monitoringSiteIdentifier,
        monitoringSiteName,
        countryCode,
        lat as latitude,
        lon as longitude
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
    WHERE 1=1
    """

    # Add filters
    if country_code:
        query += f" AND countryCode = '{country_code}'"

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        query += f" AND lon BETWEEN {min_lon} AND {max_lon}"
        query += f" AND lat BETWEEN {min_lat} AND {max_lat}"

    # Get total count for pagination
    count_query = f"SELECT COUNT(*) as total FROM ({query}) AS subquery"
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination
    query += f" LIMIT {limit} OFFSET {offset}"

    result = data_service.execute_query(query)
    flattened_data = flatten_dremio_data(result)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_spatial_locations(flattened_data, country_code)

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
    # Base query with coordinate enrichment
    query = f"""
    WITH ranked_data AS (
        SELECT
            w.*,
            s.lat as coordinate_latitude,
            s.lon as coordinate_longitude,
            s.monitoringSiteName as coordinate_siteName,
            ROW_NUMBER() OVER (
                PARTITION BY w.monitoringSiteIdentifier, w.observedPropertyDeterminandCode
                ORDER BY w.phenomenonTimeSamplingDate DESC
            ) as rn
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
        LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
            ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
    )
    SELECT *
    FROM ranked_data
    WHERE rn = 1
    """

    # Add filters
    conditions = []
    if country_code:
        conditions.append(f"countryCode = '{country_code}'")

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        conditions.append(f"coordinate_longitude BETWEEN {min_lon} AND {max_lon}")
        conditions.append(f"coordinate_latitude BETWEEN {min_lat} AND {max_lat}")

    if conditions:
        query += " AND " + " AND ".join(conditions)

    # Get total count for pagination
    count_query = f"SELECT COUNT(*) as total FROM ({query}) AS subquery"
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination
    query += f" LIMIT {limit} OFFSET {offset}"

    result = data_service.execute_query(query)
    flattened_data = flatten_dremio_data(result)
    enriched_data = format_optimized_coordinates(flattened_data)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_measurements_with_location(enriched_data)

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
    # Base query with coordinate enrichment
    query = f"""
    SELECT
        w.*,
        s.lat as coordinate_latitude,
        s.lon as coordinate_longitude,
        s.monitoringSiteName as coordinate_siteName
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
    LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
        ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
    WHERE 1=1
    """

    # Add filters
    if country_code:
        query += f" AND w.countryCode = '{country_code}'"

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        query += f" AND s.lon BETWEEN {min_lon} AND {max_lon}"
        query += f" AND s.lat BETWEEN {min_lat} AND {max_lat}"

    # Get total count for pagination
    count_query = f"SELECT COUNT(*) as total FROM ({query}) AS subquery"
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination
    query += f" LIMIT {limit} OFFSET {offset}"

    result = data_service.execute_query(query)
    flattened_data = flatten_dremio_data(result)
    enriched_data = format_optimized_coordinates(flattened_data)

    # Convert to GeoJSON
    geojson_response = GeoJSONFormatter.format_measurements_with_location(enriched_data)

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
