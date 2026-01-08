from fastapi import FastAPI, HTTPException, Query
from typing import Optional, Dict, Any
import uvicorn
import pandas as pd
from .dremio_service import DremioApiService
from .geojson_formatter import GeoJSONFormatter
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EEA WISE Data API",
    description="API service to retrieve water quality disaggregated data from the European Environment Agency (EEA) WISE_SOE database using Dremio data lake. Supports OGC-compliant GeoJSON output.",
    version="3.2.0"
)

# Initialize Dremio Data service
try:
    data_service = DremioApiService()
    service_info = data_service.get_service_info()
    logger.info(f"✓ Data service initialized successfully: {service_info['active_service']} ({service_info['service_class']})")
except Exception as e:
    logger.error(f"✗ Failed to initialize data service: {e}")
    data_service = None

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

@app.get("/healthCheck")
async def service_status():
    """Get status of data service and API features."""

    service_info = data_service.get_service_info() if data_service else {}

    return {
        "service_status": {
            "data_service_available": data_service is not None,
            "active_data_service": service_info.get('active_service', 'none'),
            "configured_mode": service_info.get('configured_mode', 'unknown')
        },
        "api_version": "3.2.0",
        "features": {
            "data_connection": data_service is not None,
            "ogc_geojson_support": True,
            "bbox_filtering": True,
            "coordinate_enrichment": "SQL JOIN (optimized)",
            "switchable_backends": True,
            "service_info": service_info
        },
    }

@app.get("/waterbase")
async def get_waterbase_data(
    country_code: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'DK', 'FI')"),
    limit: int = Query(1000, ge=1, le=300000, description="Maximum number of records to return"),
    format: str = Query("json", description="Output format: 'json' or 'geojson'")
) -> Dict[str, Any]:
    """
    Get waterbase disaggregated data.

    Args:
        country_code: Optional country code filter (e.g., 'DE' for Germany)
        limit: Maximum number of records to return (1-300000)
        format: Output format - 'json' (default) or 'geojson' (OGC-compliant)

    Returns:
        JSON or GeoJSON response with waterbase disaggregated data
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        # Get disaggregated data from Dremio with optimized coordinate inclusion
        result = data_service.get_waterbase_data(country_code, limit, include_coordinates=True)
        flattened_data = flatten_dremio_data(result)
        enriched_data = format_optimized_coordinates(flattened_data)

        # Return GeoJSON format if requested
        if format.lower() == "geojson":
            return GeoJSONFormatter.format_measurements_with_location(enriched_data)

        # Default JSON format
        return {
            "success": True,
            "data": enriched_data,
            "filters": {
                "country_code": country_code,
                "limit": limit
            },
            "metadata": {
                "total_records": len(enriched_data),
                "data_type": "disaggregated",
                "coordinates_included": True,
                "columns": result.get("columns", [])
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch waterbase data: {str(e)}"
        )


@app.get("/waterbase/country/{country_code}")
async def get_latest_measurements_by_country(
    country_code: str,
    format: str = Query("json", description="Output format: 'json' or 'geojson'")
) -> Dict[str, Any]:
    """
    Get the latest measurement for each chemical parameter by country.

    Args:
        country_code: Country code (e.g., 'FR', 'DE')
        format: Output format - 'json' (default) or 'geojson' (OGC-compliant)

    Returns:
        JSON or GeoJSON response with latest measurements per parameter for the country
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        # Get latest measurements by country from Dremio with optimized coordinate inclusion
        result = data_service.get_latest_measurements_by_country(country_code, include_coordinates=True)
        flattened_data = flatten_dremio_data(result)
        enriched_data = format_optimized_coordinates(flattened_data)

        # Return GeoJSON format if requested
        if format.lower() == "geojson":
            return GeoJSONFormatter.format_measurements_with_location(enriched_data)

        # Default JSON format
        return {
            "success": True,
            "query_type": "latest_by_country",
            "country_code": country_code,
            "source": data_service.get_service_info().get('active_service', 'unknown') if data_service else 'unknown',
            "data": enriched_data,
            "metadata": {
                "total_records": len(enriched_data),
                "coordinates_included": True,
                "description": f"Latest measurement for each chemical parameter in {country_code}"
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch latest measurements for country {country_code}: {str(e)}"
        )

@app.get("/waterbase/site/{site_identifier}")
async def get_latest_measurements_by_site(
    site_identifier: str,
    format: str = Query("json", description="Output format: 'json' or 'geojson'")
) -> Dict[str, Any]:
    """
    Get the latest measurement for each chemical parameter by monitoring site.

    Args:
        site_identifier: Monitoring site identifier (e.g., 'FRFR05026000')
        format: Output format - 'json' (default) or 'geojson' (OGC-compliant)

    Returns:
        JSON or GeoJSON response with latest measurements per parameter for the site
    """
    try:
        if not data_service:
            raise HTTPException(
                status_code=503,
                detail="Data service not available"
            )

        # Get latest measurements by site from Dremio with optimized coordinate inclusion
        result = data_service.get_latest_measurements_by_site(site_identifier, include_coordinates=True)
        flattened_data = flatten_dremio_data(result)
        enriched_data = format_optimized_coordinates(flattened_data)

        # Return GeoJSON format if requested
        if format.lower() == "geojson":
            return GeoJSONFormatter.format_measurements_with_location(enriched_data)

        # Default JSON format
        return {
            "success": True,
            "query_type": "latest_by_site",
            "site_identifier": site_identifier,
            "source": data_service.get_service_info().get('active_service', 'unknown') if data_service else 'unknown',
            "data": enriched_data,
            "metadata": {
                "total_records": len(enriched_data),
                "coordinates_included": True,
                "description": f"Latest measurement for each chemical parameter at site {site_identifier}"
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch latest measurements for site {site_identifier}: {str(e)}"
        )

@app.get("/timeseries/site/{site_identifier}")
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

@app.get("/parameters")
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

@app.get("/ogc/spatial-locations")
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


def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_server()