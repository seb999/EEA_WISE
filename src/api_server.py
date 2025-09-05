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
    description="API service to retrieve water quality data from the European Environment Agency (EEA) WISE_SOE database using Dremio data lake",
    version="3.0.0"
)

# Initialize Dremio service directly
try:
    dremio_service = DremioApiService()
    logger.info("✓ Dremio service initialized successfully")
except Exception as e:
    logger.error(f"✗ Failed to initialize Dremio service: {e}")
    dremio_service = None

# Initialize Coordinate service
try:
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


def enrich_with_coordinates(data: list) -> list:
    """
    Enrich monitoring site data with GPS coordinates placed after countryCode.
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
                    enriched_item['coordinates'] = coords
        
        enriched_data.append(enriched_item)
    
    return enriched_data

@app.get("/healthCheck")
async def service_status():
    """Get status of Dremio and coordinate services."""
    
    return {
        "service_status": {
            "dremio_available": dremio_service is not None,
            "coordinates_available": coordinate_service is not None,
            "service": "dremio_with_coordinates"
        },
        "api_version": "3.0.0",
        "features": {
            "dremio_connection": dremio_service is not None,
            "coordinate_service": coordinate_service is not None,
            "service": "dremio_with_coordinates"
        },
    }

@app.get("/waterbase")
async def get_waterbase_data(
    country: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'DK', 'FI')"),
    limit: int = Query(1000, ge=1, le=300000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """
    Get waterbase aggregated data.
    
    Args:
        country: Optional country code filter (e.g., 'DE' for Germany)
        limit: Maximum number of records to return (1-300000)
        
    Returns:
        JSON response with waterbase data
    """
    try:
        if not dremio_service:
            raise HTTPException(
                status_code=503,
                detail="Dremio service not available"
            )
            
        # Get aggregated data directly from Dremio
        result = dremio_service.get_waterbase_aggregated_data(country, limit)
        flattened_data = flatten_dremio_data(result)
        
        # Enrich with coordinates
        enriched_data = enrich_with_coordinates(flattened_data)
        
        return {
            "success": True,
            "data": enriched_data,
            "filters": {
                "country": country,
                "limit": limit
            },
            "metadata": {
                "total_records": len(enriched_data),
                "data_type": "aggregated",
                "coordinates_included": coordinate_service is not None,
                "columns": result.get("columns", [])
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch waterbase data: {str(e)}"
        )

@app.get("/waterbase/latest/disaggregated")
async def get_waterbase_disaggregated_data(
    country: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'DK', 'FI')"),
    limit: int = Query(1000, ge=1, le=300000, description="Maximum number of records to return")
) -> Dict[str, Any]:
    """
    Get waterbase disaggregated data.
    Note: Falls back to aggregated data if disaggregated data is not available.
    
    Args:
        country: Optional country code filter (e.g., 'DE' for Germany)
        limit: Maximum number of records to return (1-300000)
        
    Returns:
        JSON response with waterbase disaggregated data
    """
    try:
        if not dremio_service:
            raise HTTPException(
                status_code=503,
                detail="Dremio service not available"
            )
            
        # Get disaggregated data directly from Dremio
        result = dremio_service.get_waterbase_disaggregated_data(country, limit)
        flattened_data = flatten_dremio_data(result)
        
        # Enrich with coordinates
        enriched_data = enrich_with_coordinates(flattened_data)
        
        return {
            "success": True,
            "data": enriched_data,
            "filters": {
                "country": country,
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
            detail=f"Failed to fetch waterbase disaggregated data: {str(e)}"
        )

@app.get("/waterbase/latest/country/{country_code}")
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
        if not dremio_service:
            raise HTTPException(
                status_code=503,
                detail="Dremio service not available"
            )
            
        result = dremio_service.get_latest_measurements_by_country(country_code)
        flattened_data = flatten_dremio_data(result)
        
        # Enrich with coordinates
        enriched_data = enrich_with_coordinates(flattened_data)
        
        return {
            "success": True,
            "query_type": "latest_by_country",
            "country_code": country_code,
            "source": "Dremio Data Lake",
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

@app.get("/waterbase/latest/site/{site_identifier}")
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
        if not dremio_service:
            raise HTTPException(
                status_code=503,
                detail="Dremio service not available"
            )
            
        result = dremio_service.get_latest_measurements_by_site(site_identifier)
        flattened_data = flatten_dremio_data(result)
        
        # Enrich with coordinates
        enriched_data = enrich_with_coordinates(flattened_data)
        
        return {
            "success": True,
            "query_type": "latest_by_site",
            "site_identifier": site_identifier,
            "source": "Dremio Data Lake",
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

@app.get("/coordinates/stats")
async def get_coordinate_stats() -> Dict[str, Any]:
    """
    Get statistics about the coordinate database.
    
    Returns:
        JSON response with database statistics
    """
    try:
        if not coordinate_service:
            raise HTTPException(
                status_code=503,
                detail="Coordinate service not available"
            )
            
        stats = coordinate_service.get_database_stats()
        
        return {
            "success": True,
            "data": stats
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch coordinate statistics: {str(e)}"
        )

@app.get("/coordinates/search")
async def search_coordinates(
    q: str = Query(..., description="Search term for site names or codes"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    Search for coordinates by site name or code.
    
    Args:
        q: Search term
        limit: Maximum number of results to return
        
    Returns:
        JSON response with matching coordinates
    """
    try:
        if not coordinate_service:
            raise HTTPException(
                status_code=503,
                detail="Coordinate service not available"
            )
            
        results = coordinate_service.search_coordinates(q, limit)
        
        return {
            "success": True,
            "query": q,
            "data": results,
            "metadata": {
                "total_results": len(results),
                "search_term": q
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search coordinates: {str(e)}"
        )

@app.get("/coordinates/country/{country_code}")
async def get_coordinates_by_country(
    country_code: str,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of results")
) -> Dict[str, Any]:
    """
    Get all coordinates for a specific country.
    
    Args:
        country_code: Country code (e.g., 'DE', 'FR')
        limit: Maximum number of results to return
        
    Returns:
        JSON response with coordinates for the country
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
                "total_results": len(results)
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