"""
Utility functions for the EEA WISE Data API.

This module contains helper functions used across the API for data processing,
validation, and transformation.
"""

from typing import Dict, Any, List
from fastapi import HTTPException


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


def flatten_dremio_data(dremio_result: Dict[str, Any]) -> list:
    """
    Transform Dremio's nested {"v": "value"} format into flat dictionaries.

    Args:
        dremio_result: Result dictionary from Dremio query with 'rows' and 'columns' keys

    Returns:
        List of flattened dictionaries with column names as keys
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

    Args:
        data: List of dictionaries with coordinate_ prefixed columns

    Returns:
        List of dictionaries with coordinates formatted as nested objects
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
