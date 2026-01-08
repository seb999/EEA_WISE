"""
GeoJSON formatter for OGC-compliant spatial data output.
Converts EEA WISE monitoring site data to GeoJSON FeatureCollections.
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class GeoJSONFormatter:
    """Formatter to convert monitoring site data to OGC-compliant GeoJSON."""

    @staticmethod
    def to_feature(data: Dict[str, Any],
                   lat_field: str = 'latitude',
                   lon_field: str = 'longitude',
                   id_field: str = 'thematic_identifier') -> Optional[Dict[str, Any]]:
        """
        Convert a single data record to a GeoJSON Feature.

        Args:
            data: Dictionary containing site data
            lat_field: Field name for latitude
            lon_field: Field name for longitude
            id_field: Field name for feature ID

        Returns:
            GeoJSON Feature dictionary or None if coordinates are missing
        """
        lat = data.get(lat_field)
        lon = data.get(lon_field)

        # Skip features without valid coordinates
        if lat is None or lon is None:
            return None

        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            logger.warning(f"Invalid coordinates: lat={lat}, lon={lon}")
            return None

        # Create properties by excluding geometry fields
        properties = {}
        for key, value in data.items():
            # Skip coordinate fields and internal metadata
            if key not in [lat_field, lon_field, 'coordinate_latitude', 'coordinate_longitude',
                          'coordinate_thematic_identifier', 'coordinate_thematic_scheme',
                          'coordinate_site_name']:
                properties[key] = value

        # Build GeoJSON Feature
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]  # GeoJSON uses [lon, lat] order
            },
            "properties": properties
        }

        # Add feature ID if available
        feature_id = data.get(id_field)
        if feature_id:
            feature["id"] = feature_id

        return feature

    @staticmethod
    def to_feature_collection(data_list: List[Dict[str, Any]],
                              lat_field: str = 'latitude',
                              lon_field: str = 'longitude',
                              id_field: str = 'thematic_identifier',
                              metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Convert a list of data records to a GeoJSON FeatureCollection.

        Args:
            data_list: List of dictionaries containing site data
            lat_field: Field name for latitude
            lon_field: Field name for longitude
            id_field: Field name for feature ID
            metadata: Optional metadata to include in the response

        Returns:
            GeoJSON FeatureCollection dictionary
        """
        features = []
        skipped_count = 0

        for data in data_list:
            feature = GeoJSONFormatter.to_feature(data, lat_field, lon_field, id_field)
            if feature:
                features.append(feature)
            else:
                skipped_count += 1

        feature_collection = {
            "type": "FeatureCollection",
            "features": features
        }

        # Add optional metadata
        if metadata:
            feature_collection["metadata"] = metadata

        # Add summary information
        feature_collection["numberMatched"] = len(data_list)
        feature_collection["numberReturned"] = len(features)

        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} records without valid coordinates")

        return feature_collection

    @staticmethod
    def to_ogc_feature_collection(data_list: List[Dict[str, Any]],
                                   collection_id: str,
                                   lat_field: str = 'latitude',
                                   lon_field: str = 'longitude',
                                   id_field: str = 'thematic_identifier',
                                   links: Optional[List[Dict[str, str]]] = None,
                                   time_stamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert data to OGC API - Features compliant FeatureCollection.

        Args:
            data_list: List of dictionaries containing site data
            collection_id: Collection identifier (e.g., 'monitoring-sites')
            lat_field: Field name for latitude
            lon_field: Field name for longitude
            id_field: Field name for feature ID
            links: Optional list of related links
            time_stamp: Optional ISO 8601 timestamp

        Returns:
            OGC API - Features compliant GeoJSON FeatureCollection
        """
        features = []

        for data in data_list:
            feature = GeoJSONFormatter.to_feature(data, lat_field, lon_field, id_field)
            if feature:
                features.append(feature)

        feature_collection = {
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": len(data_list),
            "numberReturned": len(features)
        }

        # Add OGC-specific metadata
        if links:
            feature_collection["links"] = links

        if time_stamp:
            feature_collection["timeStamp"] = time_stamp

        return feature_collection

    @staticmethod
    def format_spatial_locations(spatial_data: List[Dict[str, Any]],
                                 country_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Format spatial location data from Waterbase_S_WISE_SpatialObject_DerivedData table.

        Args:
            spatial_data: List of spatial object records from Dremio
            country_code: Optional country filter for metadata

        Returns:
            GeoJSON FeatureCollection with monitoring site locations
        """
        metadata = {
            "source": "EEA WISE Waterbase Spatial Object Data",
            "table": "Waterbase_S_WISE_SpatialObject_DerivedData",
            "description": "Monitoring site locations from European Environment Agency"
        }

        if country_code:
            metadata["country_filter"] = country_code

        return GeoJSONFormatter.to_feature_collection(
            spatial_data,
            lat_field='latitude',
            lon_field='longitude',
            id_field='thematic_identifier',
            metadata=metadata
        )

    @staticmethod
    def format_measurements_with_location(measurements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format water quality measurements that include coordinate data.

        Args:
            measurements: List of measurement records with embedded coordinates

        Returns:
            GeoJSON FeatureCollection with measurements as properties
        """
        # Handle measurements that have coordinates embedded
        formatted_data = []

        for measurement in measurements:
            # Extract coordinates if they exist
            if 'coordinates' in measurement and isinstance(measurement['coordinates'], dict):
                coords = measurement['coordinates']
                flat_measurement = {
                    'latitude': coords.get('latitude'),
                    'longitude': coords.get('longitude'),
                    **{k: v for k, v in measurement.items() if k != 'coordinates'}
                }
                formatted_data.append(flat_measurement)
            elif 'coordinate_latitude' in measurement:
                # Handle flattened coordinate format
                flat_measurement = {
                    'latitude': measurement.get('coordinate_latitude'),
                    'longitude': measurement.get('coordinate_longitude'),
                    **{k: v for k, v in measurement.items()
                       if not k.startswith('coordinate_')}
                }
                formatted_data.append(flat_measurement)
            else:
                formatted_data.append(measurement)

        metadata = {
            "source": "EEA WISE Waterbase Disaggregated Data",
            "description": "Water quality measurements with monitoring site locations"
        }

        return GeoJSONFormatter.to_feature_collection(
            formatted_data,
            lat_field='latitude',
            lon_field='longitude',
            id_field='monitoringSiteIdentifier',
            metadata=metadata
        )
