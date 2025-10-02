import os
from typing import Optional, Dict, Any
import logging
from .dremio_service import DremioApiService

logger = logging.getLogger(__name__)

class CoordinateService:
    """Service to retrieve geographic coordinates for monitoring sites from Dremio spatial object table."""

    def __init__(self, dremio_service: Optional[DremioApiService] = None):
        """
        Initialize the coordinate service.

        Args:
            dremio_service: Optional existing Dremio service instance
        """
        if dremio_service:
            self.dremio_service = dremio_service
            self._owns_service = False
        else:
            try:
                self.dremio_service = DremioApiService()
                self._owns_service = True
            except Exception as e:
                logger.error(f"Failed to initialize Dremio service for coordinates: {e}")
                self.dremio_service = None
                self._owns_service = False

    def get_coordinates_for_site(self, site_id: str, country_code: str = None) -> Optional[Dict[str, Any]]:
        """
        Get coordinates for a specific monitoring site from the spatial object table.
        Uses ThematicIdIdentifier and ThematicIdIdentifierSchema to match sites.

        Args:
            site_id: Monitoring site identifier (monitoringSiteIdentifier)
            country_code: Optional country code for better matching

        Returns:
            Dictionary with latitude, longitude and other coordinate info, or None if not found
        """
        try:
            if not self.dremio_service:
                logger.warning("Dremio service not available for coordinate lookup")
                return None

            # Strategy 1: Direct match on thematicIdIdentifier with preferred scheme
            # Try euMonitoringSiteCode first (most common scheme)
            query = '''
            SELECT
                thematicIdIdentifier,
                thematicIdIdentifierScheme,
                lat,
                lon,
                monitoringSiteIdentifier,
                monitoringSiteName,
                countryCode
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
            WHERE thematicIdIdentifier = '{}'
              AND thematicIdIdentifierScheme = 'euMonitoringSiteCode'
            '''

            # Add country filter if provided
            if country_code:
                query += " AND UPPER(countryCode) = UPPER('{}')".format(country_code)
                final_query = query.format(site_id)
            else:
                final_query = query.format(site_id)

            result = self.dremio_service.execute_query(final_query)

            if result and 'rows' in result and result['rows']:
                return self._format_dremio_coordinate_result(result['rows'][0], result['columns'], 1.0, site_id)

            # Strategy 1b: If not found with euMonitoringSiteCode, try other schemes
            for scheme in ['eionetMonitoringSiteCode', 'euRBDCode', 'euSubUnitCode']:
                query_alt = '''
                SELECT
                    thematicIdIdentifier,
                    thematicIdIdentifierScheme,
                    lat,
                    lon,
                    monitoringSiteIdentifier,
                    monitoringSiteName,
                    countryCode
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
                WHERE thematicIdIdentifier = '{}'
                  AND thematicIdIdentifierScheme = '{}'
                '''

                if country_code:
                    query_alt += " AND UPPER(countryCode) = UPPER('{}')".format(country_code)
                    final_query_alt = query_alt.format(site_id, scheme)
                else:
                    final_query_alt = query_alt.format(site_id, scheme)

                result_alt = self.dremio_service.execute_query(final_query_alt)

                if result_alt and 'rows' in result_alt and result_alt['rows']:
                    logger.info(f"Found coordinate match using scheme '{scheme}' for site {site_id}")
                    return self._format_dremio_coordinate_result(result_alt['rows'][0], result_alt['columns'], 0.9, site_id)

            # Strategy 2: Try partial matching (remove trailing characters) with preferred scheme
            if len(site_id) > 6:
                base_site_id = site_id[:6]  # Use first 6 characters as base

                # Try with euMonitoringSiteCode first
                query = '''
                SELECT
                    thematicIdIdentifier,
                    thematicIdIdentifierScheme,
                    lat,
                    lon,
                    monitoringSiteIdentifier,
                    monitoringSiteName,
                    countryCode
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
                WHERE thematicIdIdentifier LIKE '{}%'
                  AND thematicIdIdentifierScheme = 'euMonitoringSiteCode'
                '''

                if country_code:
                    query += " AND UPPER(countryCode) = UPPER('{}')".format(country_code)
                    final_query = query.format(base_site_id)
                else:
                    final_query = query.format(base_site_id)

                result = self.dremio_service.execute_query(final_query)

                if result and 'rows' in result and result['rows']:
                    logger.info(f"Found coordinate match using partial matching (euMonitoringSiteCode) for site {site_id}")
                    return self._format_dremio_coordinate_result(result['rows'][0], result['columns'], 0.8, site_id)

                # Try with other schemes if not found
                for scheme in ['eionetMonitoringSiteCode', 'euRBDCode', 'euSubUnitCode']:
                    query_alt = '''
                    SELECT
                        thematicIdIdentifier,
                        thematicIdIdentifierScheme,
                        lat,
                        lon,
                        monitoringSiteIdentifier,
                        monitoringSiteName,
                        countryCode
                    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
                    WHERE thematicIdIdentifier LIKE '{}%'
                      AND thematicIdIdentifierScheme = '{}'
                    '''

                    if country_code:
                        query_alt += " AND UPPER(countryCode) = UPPER('{}')".format(country_code)
                        final_query_alt = query_alt.format(base_site_id, scheme)
                    else:
                        final_query_alt = query_alt.format(base_site_id, scheme)

                    result_alt = self.dremio_service.execute_query(final_query_alt)

                    if result_alt and 'rows' in result_alt and result_alt['rows']:
                        logger.info(f"Found coordinate match using partial matching ({scheme}) for site {site_id}")
                        return self._format_dremio_coordinate_result(result_alt['rows'][0], result_alt['columns'], 0.7, site_id)

            # Strategy 3: Country-based fallback (if country_code provided)
            # Prioritize euMonitoringSiteCode for fallback too
            if country_code:
                query = '''
                SELECT
                    thematicIdIdentifier,
                    thematicIdIdentifierScheme,
                    lat,
                    lon,
                    monitoringSiteIdentifier,
                    monitoringSiteName,
                    countryCode
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
                WHERE UPPER(countryCode) = UPPER('{}')
                  AND thematicIdIdentifierScheme = 'euMonitoringSiteCode'
                  AND lat IS NOT NULL AND lon IS NOT NULL
                LIMIT 1
                '''.format(country_code)

                result = self.dremio_service.execute_query(query)

                if result and 'rows' in result and result['rows']:
                    logger.info(f"Found fallback coordinate (euMonitoringSiteCode) for country {country_code} for site {site_id}")
                    return self._format_dremio_coordinate_result(result['rows'][0], result['columns'], 0.3, site_id)

                # Try other schemes for fallback
                for scheme in ['eionetMonitoringSiteCode', 'euRBDCode', 'euSubUnitCode']:
                    query_fallback = '''
                    SELECT
                        thematicIdIdentifier,
                        thematicIdIdentifierScheme,
                        lat,
                        lon,
                        monitoringSiteIdentifier,
                        monitoringSiteName,
                        countryCode
                    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
                    WHERE UPPER(countryCode) = UPPER('{}')
                      AND thematicIdIdentifierScheme = '{}'
                      AND lat IS NOT NULL AND lon IS NOT NULL
                    LIMIT 1
                    '''.format(country_code, scheme)

                    result_fallback = self.dremio_service.execute_query(query_fallback)

                    if result_fallback and 'rows' in result_fallback and result_fallback['rows']:
                        logger.info(f"Found fallback coordinate ({scheme}) for country {country_code} for site {site_id}")
                        return self._format_dremio_coordinate_result(result_fallback['rows'][0], result_fallback['columns'], 0.2, site_id)

            return None

        except Exception as e:
            logger.error(f"Error retrieving coordinates for site {site_id}: {e}")
            return None
    
    def _format_dremio_coordinate_result(self, row_data: Dict, columns: list, match_confidence: float, original_site_id: str) -> Dict[str, Any]:
        """Format Dremio coordinate result with simplified structure."""
        # Extract values from Dremio row format
        if isinstance(row_data, dict) and 'row' in row_data:
            values = []
            for cell in row_data['row']:
                if isinstance(cell, dict) and 'v' in cell:
                    values.append(cell['v'])
                else:
                    values.append(cell)
        else:
            values = row_data

        # Create column mapping
        column_names = [col['name'] if isinstance(col, dict) else str(col) for col in columns]
        result_dict = dict(zip(column_names, values))

        # Handle NULL or empty monitoring site names
        site_name = result_dict.get('monitoringSiteName')
        if not site_name or (isinstance(site_name, str) and site_name.strip() == ''):
            # Use thematic identifier as fallback name
            site_name = result_dict.get('thematicIdIdentifier', 'Unnamed Site')

        return {
            'latitude': result_dict.get('lat'),
            'longitude': result_dict.get('lon'),
            'thematic_identifier': result_dict.get('thematicIdIdentifier'),
            'thematic_identifier_scheme': result_dict.get('thematicIdIdentifierScheme'),
            'monitoring_site_identifier': result_dict.get('monitoringSiteIdentifier'),
            'monitoring_site_name': site_name,
            'country_code': result_dict.get('countryCode'),
            'match_confidence': match_confidence,
            'original_query_site': original_site_id
        }
    
    def get_coordinates_by_country(self, country_code: str, limit: int = 1000) -> list:
        """
        Get all coordinates for a specific country from spatial object table.

        Args:
            country_code: Country code (e.g., 'DE', 'FR')
            limit: Maximum number of results to return

        Returns:
            List of coordinate dictionaries
        """
        try:
            if not self.dremio_service:
                logger.warning("Dremio service not available for coordinate lookup")
                return []

            query = f'''
            SELECT
                thematicIdIdentifier,
                thematicIdIdentifierScheme,
                lat,
                lon,
                monitoringSiteIdentifier,
                monitoringSiteName,
                countryCode
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
            WHERE UPPER(countryCode) = UPPER('{country_code}')
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            LIMIT {limit}
            '''

            result = self.dremio_service.execute_query(query)

            if not result or 'rows' not in result:
                return []

            coordinates = []
            for row_data in result['rows']:
                try:
                    coord_result = self._format_dremio_coordinate_result(row_data, result['columns'], 1.0, "")
                    coordinates.append({
                        'thematic_identifier': coord_result['thematic_identifier'],
                        'thematic_identifier_scheme': coord_result['thematic_identifier_scheme'],
                        'monitoring_site_identifier': coord_result['monitoring_site_identifier'],
                        'monitoring_site_name': coord_result['monitoring_site_name'],
                        'latitude': coord_result['latitude'],
                        'longitude': coord_result['longitude'],
                        'country_code': coord_result['country_code']
                    })
                except Exception as e:
                    logger.warning(f"Error processing coordinate row: {e}")
                    continue

            return coordinates

        except Exception as e:
            logger.error(f"Error retrieving coordinates for country {country_code}: {e}")
            return []
    
    def search_coordinates(self, search_term: str, limit: int = 100) -> list:
        """
        Search coordinates by site identifier pattern.

        Args:
            search_term: Search term to match against ThematicIdIdentifier
            limit: Maximum number of results to return

        Returns:
            List of coordinate dictionaries
        """
        try:
            if not self.dremio_service:
                logger.warning("Dremio service not available for coordinate lookup")
                return []

            query = f'''
            SELECT
                thematicIdIdentifier,
                thematicIdIdentifierScheme,
                lat,
                lon,
                monitoringSiteIdentifier,
                monitoringSiteName,
                countryCode
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
            WHERE thematicIdIdentifier LIKE '%{search_term}%'
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            LIMIT {limit}
            '''

            result = self.dremio_service.execute_query(query)

            if not result or 'rows' not in result:
                return []

            coordinates = []
            for row_data in result['rows']:
                try:
                    coord_result = self._format_dremio_coordinate_result(row_data, result['columns'], 1.0, "")
                    coordinates.append({
                        'thematic_identifier': coord_result['thematic_identifier'],
                        'thematic_identifier_scheme': coord_result['thematic_identifier_scheme'],
                        'monitoring_site_identifier': coord_result['monitoring_site_identifier'],
                        'monitoring_site_name': coord_result['monitoring_site_name'],
                        'latitude': coord_result['latitude'],
                        'longitude': coord_result['longitude'],
                        'country_code': coord_result['country_code']
                    })
                except Exception as e:
                    logger.warning(f"Error processing coordinate row: {e}")
                    continue

            return coordinates

        except Exception as e:
            logger.error(f"Error searching coordinates with term '{search_term}': {e}")
            return []

    def close(self) -> None:
        """Close the Dremio service if owned by this instance."""
        if self._owns_service and self.dremio_service:
            self.dremio_service.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()