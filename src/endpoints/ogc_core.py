"""
OGC API - Features Core endpoints.

This module contains the core OGC API - Features Part 1 endpoints:
- Landing page (/)
- Conformance declaration (/conformance)
- Collections listing (/collections)
- Individual collection metadata (/collections/{collection_id})
- Collection items (/collections/{collection_id}/items)
"""

from fastapi import APIRouter, HTTPException, Query, Request, Header
from fastapi.responses import HTMLResponse
from typing import Optional, Dict, Any
from datetime import datetime

from ..ogc_features import OGCConformance, OGCCollections
from ..collection_handlers import (
    get_monitoring_sites_items,
    get_latest_measurements_items,
    get_disaggregated_data_items
)

# Create router
router = APIRouter()

# Initialize OGC collections (will be set by main app)
ogc_collections: OGCCollections = None
data_service = None


def init_router(collections: OGCCollections, service):
    """Initialize router with OGC collections and data service."""
    global ogc_collections, data_service
    ogc_collections = collections
    data_service = service


@router.get("/", tags=["OGC API - Features Core"])
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


@router.get("/conformance", tags=["OGC API - Features Core"])
async def get_conformance() -> Dict[str, Any]:
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


@router.get("/collections", tags=["OGC API - Features Core"])
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


@router.get("/collections/{collection_id}", tags=["OGC API - Features Core"])
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


@router.get("/collections/{collection_id}/items", tags=["OGC Collections"])
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
            return await get_monitoring_sites_items(
                data_service, request, limit, offset, bbox, country_code
            )
        elif collection_id == "latest-measurements":
            return await get_latest_measurements_items(
                data_service, request, limit, offset, bbox, country_code
            )
        elif collection_id == "disaggregated-data":
            return await get_disaggregated_data_items(
                data_service, request, limit, offset, bbox, country_code
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
