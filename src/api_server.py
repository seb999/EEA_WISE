from fastapi import FastAPI, HTTPException, Query, Request, Header
from fastapi.responses import HTMLResponse
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


def validate_bbox(bbox: str) -> tuple:
    """
    Validate and parse bounding box parameter.

    Args:
        bbox: Bounding box string in format "minLon,minLat,maxLon,maxLat"

    Returns:
        Tuple of (min_lon, min_lat, max_lon, max_lat) as floats

    Raises:
        HTTPException: If bbox format is invalid
    """
    try:
        coords = [float(x) for x in bbox.split(',')]
        if len(coords) != 4:
            raise ValueError("Expected 4 coordinates")
        min_lon, min_lat, max_lon, max_lat = coords
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError("Invalid bbox bounds: min values must be less than max values")
        return min_lon, min_lat, max_lon, max_lat
    except (ValueError, IndexError) as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid bbox format. Expected: minLon,minLat,maxLon,maxLat. Error: {str(e)}"
        )


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

@app.get("/", tags=["OGC API - Features Core"])
async def landing_page(request: Request, accept: Optional[str] = Header(None)):
    """
    Landing page providing links to API resources.

    This is the main entry point for the OGC API - Features service,
    providing links to all available resources and capabilities.
    Required by OGC API - Features Part 1: Core.

    Supports content negotiation:
    - Accept: application/json - Returns JSON (default)
    - Accept: text/html - Returns HTML visual layout

    Returns:
        Dictionary with API metadata and links to resources, or HTML page

    Example:
        GET /
    """
    base_url = str(request.base_url).rstrip('/')

    # Prepare links data
    links_data = [
        {
            "href": f"{base_url}/",
            "rel": "self",
            "type": "application/json",
            "title": "This document"
        },
        {
            "href": f"{base_url}/openapi.json",
            "rel": "service-desc",
            "type": "application/vnd.oai.openapi+json;version=3.0",
            "title": "API definition in OpenAPI 3.0"
        },
        {
            "href": f"{base_url}/docs",
            "rel": "service-doc",
            "type": "text/html",
            "title": "API documentation (Swagger UI)"
        },
        {
            "href": f"{base_url}/conformance",
            "rel": "conformance",
            "type": "application/json",
            "title": "OGC API conformance classes implemented by this service"
        },
        {
            "href": f"{base_url}/collections",
            "rel": "data",
            "type": "application/json",
            "title": "Feature collections available from this service"
        }
    ]

    # Check if HTML is preferred (browser request)
    if accept and "text/html" in accept and "application/json" not in accept.split(";")[0]:
        # Return HTML visual landing page
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>WISE Water Information System - Data API</title>
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    background: #f5f5f5;
                    color: #333;
                    line-height: 1.6;
                }}
                .top-bar {{
                    background: #003d5c;
                    color: white;
                    padding: 0.5rem 0;
                }}
                .top-bar .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 2rem;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    font-size: 0.85rem;
                }}
                .header {{
                    background: #0077b3;
                    color: white;
                    padding: 2rem 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .header .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 2rem;
                }}
                .header h1 {{
                    font-size: 2.2rem;
                    margin-bottom: 0.5rem;
                    font-weight: 600;
                }}
                .header .subtitle {{
                    font-size: 1.1rem;
                    opacity: 0.95;
                    font-weight: 300;
                }}
                .nav {{
                    background: white;
                    border-bottom: 1px solid #e0e0e0;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
                }}
                .nav .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 2rem;
                    display: flex;
                    gap: 2rem;
                }}
                .nav a {{
                    color: #003d5c;
                    text-decoration: none;
                    padding: 1rem 0;
                    display: inline-block;
                    border-bottom: 3px solid transparent;
                    transition: all 0.2s ease;
                    font-weight: 500;
                }}
                .nav a:hover {{
                    color: #0077b3;
                    border-bottom-color: #0077b3;
                }}
                .main {{
                    max-width: 1200px;
                    margin: 2rem auto;
                    padding: 0 2rem;
                }}
                .hero-banner {{
                    background: white;
                    border-left: 4px solid #0077b3;
                    padding: 1.5rem;
                    margin-bottom: 2rem;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
                }}
                .hero-banner h2 {{
                    color: #003d5c;
                    font-size: 1.3rem;
                    margin-bottom: 0.5rem;
                }}
                .hero-banner .badge {{
                    display: inline-block;
                    background: #28a745;
                    color: white;
                    padding: 0.25rem 0.6rem;
                    border-radius: 3px;
                    font-size: 0.8rem;
                    font-weight: 600;
                    margin-left: 0.5rem;
                }}
                .section {{
                    margin-bottom: 3rem;
                }}
                .section h2 {{
                    color: #003d5c;
                    font-size: 1.8rem;
                    margin-bottom: 1.5rem;
                    font-weight: 600;
                }}
                .cards-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 1.5rem;
                }}
                .card {{
                    background: white;
                    border: 1px solid #e0e0e0;
                    border-radius: 2px;
                    overflow: hidden;
                    transition: all 0.3s ease;
                    text-decoration: none;
                    color: inherit;
                    display: block;
                }}
                .card:hover {{
                    box-shadow: 0 4px 12px rgba(0,119,179,0.15);
                    border-color: #0077b3;
                    transform: translateY(-2px);
                }}
                .card-header {{
                    background: #f8f9fa;
                    padding: 1rem 1.25rem;
                    border-bottom: 1px solid #e0e0e0;
                }}
                .card-header h3 {{
                    color: #0077b3;
                    font-size: 1.1rem;
                    font-weight: 600;
                    margin: 0;
                }}
                .card-body {{
                    padding: 1.25rem;
                }}
                .card-body p {{
                    color: #555;
                    font-size: 0.95rem;
                    margin-bottom: 0.75rem;
                }}
                .card-footer {{
                    padding: 0 1.25rem 1.25rem;
                    font-size: 0.85rem;
                    color: #777;
                    font-family: 'Courier New', monospace;
                }}
                .info-panel {{
                    background: #e8f4f8;
                    border-left: 4px solid #0077b3;
                    padding: 1.25rem;
                    margin: 1.5rem 0;
                }}
                .info-panel p {{
                    color: #003d5c;
                    margin: 0.5rem 0;
                }}
                .info-panel code {{
                    background: white;
                    padding: 0.2rem 0.5rem;
                    border: 1px solid #d0d0d0;
                    border-radius: 2px;
                    font-family: 'Courier New', monospace;
                    font-size: 0.9rem;
                }}
                .footer {{
                    background: #003d5c;
                    color: white;
                    padding: 2rem 0;
                    margin-top: 4rem;
                }}
                .footer .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 2rem;
                }}
                .footer-content {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 2rem;
                    margin-bottom: 2rem;
                }}
                .footer-section h4 {{
                    color: #7ec8e3;
                    font-size: 1rem;
                    margin-bottom: 0.75rem;
                    font-weight: 600;
                }}
                .footer-section ul {{
                    list-style: none;
                }}
                .footer-section a {{
                    color: #b8d9ea;
                    text-decoration: none;
                    font-size: 0.9rem;
                    line-height: 2;
                }}
                .footer-section a:hover {{
                    color: white;
                    text-decoration: underline;
                }}
                .footer-bottom {{
                    border-top: 1px solid #004d73;
                    padding-top: 1.5rem;
                    text-align: center;
                    font-size: 0.85rem;
                    color: #b8d9ea;
                }}
            </style>
        </head>
        <body>
            <div class="top-bar">
                <div class="container">
                    <span>European Environment Agency</span>
                    <span>OGC API - Features Compliant Service</span>
                </div>
            </div>

            <div class="header">
                <div class="container">
                    <h1>WISE Water Information System</h1>
                    <div class="subtitle">Data API Service for European Water Quality Information</div>
                </div>
            </div>

            <div class="nav">
                <div class="container">
                    <a href="{base_url}/collections">Collections</a>
                    <a href="{base_url}/docs">Documentation</a>
                    <a href="{base_url}/conformance">Conformance</a>
                    <a href="{base_url}/openapi.json">OpenAPI</a>
                </div>
            </div>

            <div class="main">
                <div class="hero-banner">
                    <h2>Water Quality Data API <span class="badge">OGC v1.0</span></h2>
                    <p>Access disaggregated water quality data from the WISE_SOE database via standardized OGC API - Features endpoints. This service provides interoperable geospatial data for monitoring sites, measurements, and disaggregated datasets across Europe.</p>
                </div>

                <div class="section">
                    <h2>API Resources</h2>
                    <div class="cards-grid">
                        <a href="{base_url}/collections" class="card">
                            <div class="card-header">
                                <h3>Collections</h3>
                            </div>
                            <div class="card-body">
                                <p>Browse available feature collections including monitoring sites, latest measurements, and complete disaggregated water quality data</p>
                            </div>
                            <div class="card-footer">GET /collections</div>
                        </a>

                        <a href="{base_url}/conformance" class="card">
                            <div class="card-header">
                                <h3>Conformance</h3>
                            </div>
                            <div class="card-body">
                                <p>View OGC API - Features conformance classes and standards implemented by this service</p>
                            </div>
                            <div class="card-footer">GET /conformance</div>
                        </a>

                        <a href="{base_url}/docs" class="card">
                            <div class="card-header">
                                <h3>API Documentation</h3>
                            </div>
                            <div class="card-body">
                                <p>Interactive Swagger UI documentation with request examples and response schemas</p>
                            </div>
                            <div class="card-footer">GET /docs</div>
                        </a>

                        <a href="{base_url}/openapi.json" class="card">
                            <div class="card-header">
                                <h3>OpenAPI Specification</h3>
                            </div>
                            <div class="card-body">
                                <p>Download the complete OpenAPI 3.0 specification document for this API</p>
                            </div>
                            <div class="card-footer">GET /openapi.json</div>
                        </a>
                    </div>
                </div>

                <div class="section">
                    <h2>Available Data Collections</h2>
                    <div class="cards-grid">
                        <a href="{base_url}/collections/monitoring-sites/items?limit=10" class="card">
                            <div class="card-header">
                                <h3>Monitoring Sites</h3>
                            </div>
                            <div class="card-body">
                                <p>Spatial locations of water quality monitoring sites with geographic coordinates across European countries</p>
                            </div>
                        </a>

                        <a href="{base_url}/collections/latest-measurements/items?limit=10" class="card">
                            <div class="card-header">
                                <h3>Latest Measurements</h3>
                            </div>
                            <div class="card-body">
                                <p>Most recent water quality measurements per parameter at each monitoring site location</p>
                            </div>
                        </a>

                        <a href="{base_url}/collections/disaggregated-data/items?limit=10" class="card">
                            <div class="card-header">
                                <h3>Disaggregated Data</h3>
                            </div>
                            <div class="card-body">
                                <p>Complete water quality measurement dataset with full metadata and observational properties</p>
                            </div>
                        </a>
                    </div>
                </div>

                <div class="section">
                    <h2>Query Examples</h2>
                    <div class="info-panel">
                        <p><strong>Filter by country (ISO code):</strong></p>
                        <p><code>{base_url}/collections/monitoring-sites/items?country_code=FR</code></p>

                        <p style="margin-top: 1rem;"><strong>Spatial bounding box query:</strong></p>
                        <p><code>{base_url}/collections/monitoring-sites/items?bbox=2.2,48.8,2.5,48.9</code></p>

                        <p style="margin-top: 1rem;"><strong>Pagination with limit and offset:</strong></p>
                        <p><code>{base_url}/collections/monitoring-sites/items?limit=100&offset=0</code></p>
                    </div>
                </div>
            </div>

            <div class="footer">
                <div class="container">
                    <div class="footer-content">
                        <div class="footer-section">
                            <h4>About WISE</h4>
                            <ul>
                                <li><a href="https://water.europa.eu">Water Information System for Europe</a></li>
                                <li><a href="https://www.eea.europa.eu">European Environment Agency</a></li>
                            </ul>
                        </div>
                        <div class="footer-section">
                            <h4>Technical Information</h4>
                            <ul>
                                <li><a href="{base_url}/docs">API Documentation</a></li>
                                <li><a href="{base_url}/openapi.json">OpenAPI 3.0 Specification</a></li>
                                <li><a href="https://ogcapi.ogc.org/features/">OGC API - Features Standard</a></li>
                            </ul>
                        </div>
                        <div class="footer-section">
                            <h4>Data Source</h4>
                            <ul>
                                <li>WISE_SOE Database</li>
                                <li>Dremio Data Lake</li>
                                <li>Powered by FastAPI</li>
                            </ul>
                        </div>
                    </div>
                    <div class="footer-bottom">
                        <p>&copy; European Environment Agency | OGC API - Features Part 1: Core Compliant</p>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html_content)

    # Return JSON for API clients (default)
    return {
        "title": "EEA WISE Data API",
        "description": "API service to retrieve water quality disaggregated data from the European Environment Agency (EEA) WISE_SOE database using Dremio data lake. Supports OGC API - Features compliance with GeoJSON output.",
        "links": links_data
    }


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


def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_server()