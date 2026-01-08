from fastapi import FastAPI, HTTPException, Query, Request
from typing import Optional, Dict, Any, List
import uvicorn
from .dremio_service import DremioApiService
from .geojson_formatter import GeoJSONFormatter
from .ogc_features import OGCConformance, OGCCollections, OGCLinks
import logging
from datetime import datetime

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

def flatten_dremio_data(dremio_result: Dict[str, Any]) -> list:
    """
    Transform Dremio's nested {"v": "value"} format into flat dictionaries.
    """
    if not dremio_result.get("rows") or not dremio_result.get("columns"):
        return []
    
    columns = dremio_result["columns"]
    rows = dremio_result["rows"]
    flattened_data = []
    
    for row_data in rows:
        if isinstance(row_data, dict) and "row" in row_data:
            # Handle {"row": [{"v": "value"}, ...]} format
            row_values = row_data["row"]
            flattened_row = {}
            
            for i, col_info in enumerate(columns):
                col_name = col_info.get("name", f"col_{i}")
                if i < len(row_values):
                    value_obj = row_values[i]
                    if isinstance(value_obj, dict) and "v" in value_obj:
                        flattened_row[col_name] = value_obj["v"]
                    else:
                        flattened_row[col_name] = value_obj
                else:
                    flattened_row[col_name] = None
                    
            flattened_data.append(flattened_row)
        elif isinstance(row_data, list):
            # Handle direct array format
            flattened_row = {}
            for i, col_info in enumerate(columns):
                col_name = col_info.get("name", f"col_{i}")
                if i < len(row_data):
                    flattened_row[col_name] = row_data[i]
                else:
                    flattened_row[col_name] = None
                    
            flattened_data.append(flattened_row)
    
    return flattened_data


def format_optimized_coordinates(data: list) -> list:
    """
    Format data that already includes coordinates from JOIN query.
    Transforms coordinate columns into structured coordinate objects.
    """
    if not data:
        return data

    formatted_data = []
    for item in data:
        # Create new ordered dict with formatted coordinates
        formatted_item = {}

        for key, value in item.items():
            # Skip coordinate column fields as we'll format them into coordinates object
            if key.startswith('coordinate_'):
                continue

            formatted_item[key] = value

            # Insert coordinates right after countryCode
            if key == 'countryCode':
                # Check if we have coordinate data from the JOIN
                lat = item.get('coordinate_latitude')
                lon = item.get('coordinate_longitude')

                if lat is not None and lon is not None:
                    formatted_item['coordinates'] = {
                        'latitude': lat,
                        'longitude': lon,
                        'thematic_identifier': item.get('coordinate_thematic_identifier'),
                        'thematic_identifier_scheme': item.get('coordinate_thematic_scheme'),
                        'monitoring_site_name': item.get('coordinate_site_name'),
                        'match_confidence': 1.0,  # High confidence as this is a direct JOIN match
                        'source': 'Waterbase_S_WISE_SpatialObject_DerivedData'
                    }
                else:
                    # No coordinates found in JOIN
                    formatted_item['coordinates'] = None

        formatted_data.append(formatted_item)

    return formatted_data

@app.get("/healthCheck", tags=["System"])
async def service_status():
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
            "collections": ogc_collections.list_collection_ids()
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

@app.get("/timeseries/site/{site_identifier}", tags=["Time-Series"])
async def get_timeseries_by_site(
    site_identifier: str,
    parameter: Optional[str] = Query(None, description="Chemical parameter code (e.g., 'NO3')"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    interval: str = Query("raw", description="Aggregation interval: 'raw', 'monthly', 'yearly'")
) -> Dict[str, Any]:
    """
    Get time-series data for a specific monitoring site.

    Args:
        site_identifier: Monitoring site identifier (e.g., 'FRFR05026000')
        parameter: Optional chemical parameter code filter
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        interval: Data aggregation interval ('raw', 'monthly', 'yearly')

    Returns:
        JSON response with time-series data for the site
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        # Validate interval
        if interval not in ['raw', 'monthly', 'yearly']:
            raise HTTPException(
                status_code=400,
                detail="Invalid interval. Must be 'raw', 'monthly', or 'yearly'"
            )

        # Get time-series data from Dremio with coordinates
        result = data_service.get_timeseries_by_site(
            site_identifier=site_identifier,
            parameter_code=parameter,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            include_coordinates=True
        )

        flattened_data = flatten_dremio_data(result)

        # Format coordinates for aggregated data
        if interval != 'raw':
            enriched_data = format_optimized_coordinates(flattened_data)
        else:
            enriched_data = format_optimized_coordinates(flattened_data)

        return {
            "success": True,
            "query_type": "timeseries",
            "site_identifier": site_identifier,
            "filters": {
                "parameter": parameter,
                "start_date": start_date,
                "end_date": end_date,
                "interval": interval
            },
            "data": enriched_data,
            "metadata": {
                "total_records": len(enriched_data),
                "coordinates_included": True,
                "aggregation_interval": interval,
                "description": f"Time-series data for site {site_identifier}"
            }
        }

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch time-series data for site {site_identifier}: {str(e)}"
        )

@app.get("/parameters", tags=["Metadata"])
async def get_available_parameters() -> Dict[str, Any]:
    """
    Get list of available chemical parameters with metadata.

    Returns:
        JSON response with available chemical parameters
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        result = data_service.get_available_parameters()
        flattened_data = flatten_dremio_data(result)

        return {
            "success": True,
            "data": flattened_data,
            "metadata": {
                "total_parameters": len(flattened_data),
                "description": "Available chemical parameters in the WISE database"
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch available parameters: {str(e)}"
        )

@app.get("/ogc/spatial-locations", tags=["Legacy OGC"])
async def get_ogc_spatial_locations(
    country_code: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'FR')"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of features to return"),
    bbox: Optional[str] = Query(None, description="Bounding box filter: minLon,minLat,maxLon,maxLat")
) -> Dict[str, Any]:
    """
    OGC-compliant endpoint to retrieve monitoring site locations as GeoJSON.

    This endpoint returns spatial locations from the Waterbase_S_WISE_SpatialObject_DerivedData table
    in OGC-compliant GeoJSON FeatureCollection format.

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
            try:
                min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(','))
                base_query += f" AND lon >= {min_lon} AND lon <= {max_lon}"
                base_query += f" AND lat >= {min_lat} AND lat <= {max_lat}"
            except (ValueError, IndexError):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid bbox format. Use: minLon,minLat,maxLon,maxLat"
                )

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


# ============================================================================
# OGC API - Features Endpoints (Phase 2)
# ============================================================================

@app.get("/conformance", tags=["OGC API - Features Core"])
async def get_conformance() -> Dict[str, List[str]]:
    """
    OGC API - Features conformance declaration.

    Declares which OGC conformance classes this API implements.
    This is a core requirement of OGC API - Features Part 1.

    Returns:
        Dictionary with conformsTo array listing implemented conformance classes

    Example:
        GET /conformance
    """
    return OGCConformance.get_conformance_declaration()


@app.get("/collections", tags=["OGC API - Features Core"])
async def get_collections(request: Request) -> Dict[str, Any]:
    """
    Get list of available OGC API - Features collections.

    Returns metadata about all available collections (datasets) that can be
    queried via the /collections/{collectionId}/items endpoint.

    Returns:
        Dictionary with collections array and links

    Example:
        GET /collections
    """
    # Build base URL from request
    base_url = str(request.base_url).rstrip('/')

    return ogc_collections.get_all_collections(base_url)


@app.get("/collections/{collection_id}", tags=["OGC API - Features Core"])
async def get_collection(collection_id: str, request: Request) -> Dict[str, Any]:
    """
    Get metadata for a specific collection.

    Args:
        collection_id: Collection identifier (e.g., 'monitoring-sites', 'latest-measurements')

    Returns:
        Collection metadata dictionary

    Raises:
        HTTPException: If collection not found

    Example:
        GET /collections/monitoring-sites
    """
    collection = ogc_collections.get_collection(collection_id)

    if not collection:
        available = ogc_collections.list_collection_ids()
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found. Available collections: {', '.join(available)}"
        )

    base_url = str(request.base_url).rstrip('/')
    return collection.to_dict(base_url)


@app.get("/collections/{collection_id}/items", tags=["OGC Collections"])
async def get_collection_items(
    collection_id: str,
    request: Request,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    bbox: Optional[str] = Query(None, description="Bounding box filter: minLon,minLat,maxLon,maxLat"),
    country_code: Optional[str] = Query(None, description="Filter by ISO country code"),
    datetime_param: Optional[str] = Query(None, alias="datetime", description="Temporal filter (ISO 8601 interval)")
) -> Dict[str, Any]:
    """
    Get items (features) from a collection.

    This is the main OGC API - Features endpoint for querying data.
    Supports spatial filtering (bbox), country filtering, and temporal filtering.

    **Available Collections:**
    - `monitoring-sites` - Monitoring site locations
    - `latest-measurements` - Latest measurements per parameter
    - `disaggregated-data` - Complete water quality data

    **Note:** Time-series data is available via the dedicated `/timeseries/site/{site_identifier}` endpoint
    with specialized aggregation features (raw/monthly/yearly).

    Args:
        collection_id: Collection identifier (monitoring-sites, latest-measurements, disaggregated-data)
        limit: Maximum number of items to return (1-10000)
        offset: Number of items to skip (for pagination)
        bbox: Bounding box as 'minLon,minLat,maxLon,maxLat'
        country_code: ISO country code filter (e.g., 'FR', 'DE')
        datetime_param: Temporal filter in ISO 8601 format

    Returns:
        GeoJSON FeatureCollection with items

    Examples:
        GET /collections/monitoring-sites/items?country_code=FR&limit=100
        GET /collections/latest-measurements/items?bbox=2.2,48.8,2.5,48.9
        GET /collections/disaggregated-data/items?country_code=FR&limit=1000&offset=0
    """
    if not data_service:
        raise HTTPException(status_code=503, detail="Data service unavailable")

    # Validate collection exists
    collection = ogc_collections.get_collection(collection_id)
    if not collection:
        available = ogc_collections.list_collection_ids()
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_id}' not found. Available collections: {', '.join(available)}"
        )

    try:
        # Route to appropriate handler based on collection_id
        if collection_id == "monitoring-sites":
            return await _get_monitoring_sites_items(
                request, limit, offset, bbox, country_code
            )
        elif collection_id == "latest-measurements":
            return await _get_latest_measurements_items(
                request, limit, offset, bbox, country_code
            )
        elif collection_id == "disaggregated-data":
            return await _get_disaggregated_data_items(
                request, limit, offset, bbox, country_code
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Collection handler not implemented for '{collection_id}'"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch collection items: {str(e)}"
        )


async def _get_monitoring_sites_items(
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """Helper function to get monitoring sites collection items."""

    query = f"""
    SELECT
        thematicIdIdentifier,
        thematicIdIdentifierScheme,
        monitoringSiteIdentifier,
        monitoringSiteName,
        countryCode,
        lat,
        lon
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
    WHERE 1=1
    """

    # Add filters
    if country_code:
        query += f" AND countryCode = '{country_code}'"

    if bbox:
        try:
            coords = [float(x) for x in bbox.split(',')]
            if len(coords) != 4:
                raise ValueError()
            min_lon, min_lat, max_lon, max_lat = coords
            query += f" AND lon BETWEEN {min_lon} AND {max_lon}"
            query += f" AND lat BETWEEN {min_lat} AND {max_lat}"
        except (ValueError, IndexError):
            raise HTTPException(
                status_code=400,
                detail="Invalid bbox format. Expected: minLon,minLat,maxLon,maxLat"
            )

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

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response


async def _get_latest_measurements_items(
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """Helper function to get latest measurements collection items."""

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
        try:
            coords = [float(x) for x in bbox.split(',')]
            if len(coords) != 4:
                raise ValueError()
            min_lon, min_lat, max_lon, max_lat = coords
            conditions.append(f"coordinate_longitude BETWEEN {min_lon} AND {max_lon}")
            conditions.append(f"coordinate_latitude BETWEEN {min_lat} AND {max_lat}")
        except (ValueError, IndexError):
            raise HTTPException(
                status_code=400,
                detail="Invalid bbox format. Expected: minLon,minLat,maxLon,maxLat"
            )

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

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response


async def _get_disaggregated_data_items(
    request: Request,
    limit: int,
    offset: int,
    bbox: Optional[str],
    country_code: Optional[str]
) -> Dict[str, Any]:
    """Helper function to get disaggregated data collection items."""

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
        try:
            coords = [float(x) for x in bbox.split(',')]
            if len(coords) != 4:
                raise ValueError()
            min_lon, min_lat, max_lon, max_lat = coords
            query += f" AND s.lon BETWEEN {min_lon} AND {max_lon}"
            query += f" AND s.lat BETWEEN {min_lat} AND {max_lat}"
        except (ValueError, IndexError):
            raise HTTPException(
                status_code=400,
                detail="Invalid bbox format. Expected: minLon,minLat,maxLon,maxLat"
            )

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

    # Add OGC metadata
    geojson_response["numberMatched"] = total_count
    geojson_response["timeStamp"] = datetime.utcnow().isoformat() + "Z"

    return geojson_response


def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_server()