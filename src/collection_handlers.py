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
    # Build WHERE conditions first for optimal filtering
    where_conditions = []

    if country_code:
        where_conditions.append(f"countryCode = '{country_code}'")

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        where_conditions.append(f"lon BETWEEN {min_lon} AND {max_lon}")
        where_conditions.append(f"lat BETWEEN {min_lat} AND {max_lat}")

    # Always filter for non-null coordinates
    where_conditions.append("lat IS NOT NULL")
    where_conditions.append("lon IS NOT NULL")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # Optimized query - filter first, then select
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
    {where_clause}
    """

    # Get total count - use COUNT(*) directly for better performance
    count_query = f"""
    SELECT COUNT(*) as total
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
    {where_clause}
    """
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination to main query
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
    # Build WHERE conditions for filtering BEFORE window function
    where_conditions = []

    if country_code:
        where_conditions.append(f"w.countryCode = '{country_code}'")

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        where_conditions.append(f"s.lon BETWEEN {min_lon} AND {max_lon}")
        where_conditions.append(f"s.lat BETWEEN {min_lat} AND {max_lat}")

    # Always filter for non-null coordinates and dates
    where_conditions.append("s.lat IS NOT NULL")
    where_conditions.append("s.lon IS NOT NULL")
    where_conditions.append("w.phenomenonTimeSamplingDate IS NOT NULL")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # Optimized query - filter FIRST, then apply window function
    query = f"""
    WITH filtered_data AS (
        SELECT
            w.*,
            s.lat as coordinate_latitude,
            s.lon as coordinate_longitude,
            s.monitoringSiteName as coordinate_siteName
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
        INNER JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
            ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
            AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
        {where_clause}
    ),
    ranked_data AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY monitoringSiteIdentifier, observedPropertyDeterminandCode
                ORDER BY phenomenonTimeSamplingDate DESC
            ) as rn
        FROM filtered_data
    )
    SELECT *
    FROM ranked_data
    WHERE rn = 1
    """

    # Optimized count query - count filtered data before ranking
    count_query = f"""
    WITH filtered_data AS (
        SELECT
            w.monitoringSiteIdentifier,
            w.observedPropertyDeterminandCode
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
        INNER JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
            ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
            AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
        {where_clause}
    )
    SELECT COUNT(DISTINCT monitoringSiteIdentifier || '|' || observedPropertyDeterminandCode) as total
    FROM filtered_data
    """
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination to main query
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
    # Build WHERE conditions first for optimal filtering
    where_conditions = []

    if country_code:
        where_conditions.append(f"w.countryCode = '{country_code}'")

    if bbox:
        min_lon, min_lat, max_lon, max_lat = validate_bbox(bbox)
        where_conditions.append(f"s.lon BETWEEN {min_lon} AND {max_lon}")
        where_conditions.append(f"s.lat BETWEEN {min_lat} AND {max_lat}")

    # Always filter for non-null coordinates
    where_conditions.append("s.lat IS NOT NULL")
    where_conditions.append("s.lon IS NOT NULL")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    # Optimized query - use INNER JOIN and filter early
    query = f"""
    SELECT
        w.*,
        s.lat as coordinate_latitude,
        s.lon as coordinate_longitude,
        s.monitoringSiteName as coordinate_siteName
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
    INNER JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
        ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
        AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
    {where_clause}
    """

    # Optimized count query - count only filtered rows
    count_query = f"""
    SELECT COUNT(*) as total
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
    INNER JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
        ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
        AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
    {where_clause}
    """
    count_result = data_service.execute_query(count_query)
    count_data = flatten_dremio_data(count_result)
    total_count = count_data[0]['total'] if count_data else 0

    # Add pagination to main query
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
