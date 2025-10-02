from fastapi import FastAPI, HTTPException, Query
from typing import Optional, Dict, Any
import uvicorn
import pandas as pd
from .dremio_service import DremioApiService
from .coordinate_service import CoordinateService
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EEA WISE Data API",
    description="API service to retrieve water quality disaggregated data from the European Environment Agency (EEA) WISE_SOE database using Dremio data lake",
    version="3.1.0"
)

# Initialize Dremio Data service
try:
    data_service = DremioApiService()
    service_info = data_service.get_service_info()
    logger.info(f"✓ Data service initialized successfully: {service_info['active_service']} ({service_info['service_class']})")
except Exception as e:
    logger.error(f"✗ Failed to initialize data service: {e}")
    data_service = None

# Initialize Coordinate service with shared Dremio service
try:
    if data_service:
        coordinate_service = CoordinateService(data_service)
    else:
        coordinate_service = CoordinateService()
    logger.info("✓ Coordinate service initialized successfully")
except Exception as e:
    logger.error(f"✗ Failed to initialize Coordinate service: {e}")
    coordinate_service = None

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

def enrich_with_coordinates(data: list) -> list:
    """
    Enrich monitoring site data with GPS coordinates from spatial object table.
    Coordinates are placed after countryCode and use ThematicIdIdentifier matching.
    """
    if not coordinate_service or not data:
        return data

    enriched_data = []
    for item in data:
        site_id = item.get('monitoringSiteIdentifier')
        country_code = item.get('countryCode')

        # Create new ordered dict with coordinates after countryCode
        enriched_item = {}

        for key, value in item.items():
            enriched_item[key] = value

            # Insert coordinates right after countryCode
            if key == 'countryCode' and site_id and country_code:
                coords = coordinate_service.get_coordinates_for_site(site_id, country_code)
                if coords:
                    # Format coordinates with spatial object information
                    enriched_item['coordinates'] = {
                        'latitude': coords.get('latitude'),
                        'longitude': coords.get('longitude'),
                        'thematic_identifier': coords.get('thematic_identifier'),
                        'thematic_identifier_scheme': coords.get('thematic_identifier_scheme'),
                        'monitoring_site_identifier': coords.get('monitoring_site_identifier'),
                        'monitoring_site_name': coords.get('monitoring_site_name'),
                        'match_confidence': coords.get('match_confidence', 'unknown'),
                        'original_query_site': coords.get('original_query_site'),
                        'source': 'Waterbase_S_WISE_SpatialObject_DerivedData'
                    }

        enriched_data.append(enriched_item)

    return enriched_data

@app.get("/healthCheck")
async def service_status():
    """Get status of data and coordinate services."""

    service_info = data_service.get_service_info() if data_service else {}

    return {
        "service_status": {
            "data_service_available": data_service is not None,
            "coordinates_available": coordinate_service is not None,
            "active_data_service": service_info.get('active_service', 'none'),
            "configured_mode": service_info.get('configured_mode', 'unknown')
        },
        "api_version": "3.1.0",
        "features": {
            "data_connection": data_service is not None,
            "coordinate_service": coordinate_service is not None,
            "switchable_backends": True,
            "service_info": service_info
        },
    }

@app.get("/waterbase")
async def get_waterbase_data(
    country_code: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'DK', 'FI')"),
    limit: int = Query(1000, ge=1, le=300000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """
    Get waterbase disaggregated data.

    Args:
        country: Optional country code filter (e.g., 'DE' for Germany)
        limit: Maximum number of records to return (1-300000)

    Returns:
        JSON response with waterbase disaggregated data
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
                "coordinates_included": coordinate_service is not None,
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
    country_code: str
) -> Dict[str, Any]:
    """
    Get the latest measurement for each chemical parameter by country.
    
    Args:
        country_code: Country code (e.g., 'FR', 'DE')
        
    Returns:
        JSON response with latest measurements per parameter for the country
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
        
        return {
            "success": True,
            "query_type": "latest_by_country",
            "country_code": country_code,
            "source": data_service.get_service_info().get('active_service', 'unknown') if data_service else 'unknown',
            "data": enriched_data,
            "metadata": {
                "total_records": len(enriched_data),
                "coordinates_included": coordinate_service is not None,
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
    site_identifier: str
) -> Dict[str, Any]:
    """
    Get the latest measurement for each chemical parameter by monitoring site.
    Args:
        site_identifier: Monitoring site identifier (e.g., 'FRFR05026000')
    Returns:
        JSON response with latest measurements per parameter for the site
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
        
        return {
            "success": True,
            "query_type": "latest_by_site",
            "site_identifier": site_identifier,
            "source": data_service.get_service_info().get('active_service', 'unknown') if data_service else 'unknown',
            "data": enriched_data,
            "metadata": {
                "total_records": len(enriched_data),
                "coordinates_included": coordinate_service is not None,
                "description": f"Latest measurement for each chemical parameter at site {site_identifier}"
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch latest measurements for site {site_identifier}: {str(e)}"
        )

async def get_coordinates_by_country(
    country_code: str,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    Get all sites with coordinates for a specific country from spatial object table.
    Args:
        country_code: Country code (e.g., 'DE', 'FR')
        limit: Maximum number of results to return
    Returns:
        JSON response with coordinates from Waterbase_S_WISE_SpatialObject_DerivedData
    """
    try:
        if not coordinate_service:
            raise HTTPException(
                status_code=503,
                detail="Coordinate service not available"
            )

        results = coordinate_service.get_coordinates_by_country(country_code.upper(), limit)

        return {
            "success": True,
            "country_code": country_code.upper(),
            "data": results,
            "metadata": {
                "total_results": len(results),
                "data_source": "Waterbase_S_WISE_SpatialObject_DerivedData",
                "description": f"GPS coordinates for monitoring sites in {country_code.upper()} using ThematicIdIdentifier matching"
            }
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch coordinates for country {country_code}: {str(e)}"
        )

def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_server()