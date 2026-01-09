"""
Time-series endpoints for water quality data.

This module contains endpoints for retrieving time-series water quality data
with aggregation capabilities (raw, monthly, yearly).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any

from ..utils import flatten_dremio_data, format_optimized_coordinates

# Create router
router = APIRouter()

# Data service will be set by main app
data_service = None


def init_router(service):
    """Initialize router with data service."""
    global data_service
    data_service = service


@router.get("/timeseries/site/{site_identifier}", tags=["Time-Series"])
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
