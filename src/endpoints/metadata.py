"""
Metadata discovery endpoints.

This module contains endpoints for discovering available parameters,
monitoring sites, and other metadata about the water quality database.
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from ..utils import flatten_dremio_data

# Create router
router = APIRouter()

# Data service will be set by main app
data_service = None


def init_router(service):
    """Initialize router with data service."""
    global data_service
    data_service = service


@router.get("/parameters", tags=["Metadata"])
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
