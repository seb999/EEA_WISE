import requests
import pandas as pd
from typing import Optional, Dict, Any, List
import json
from urllib.parse import urljoin
import os
from dotenv import load_dotenv

load_dotenv()

class DremioApiService:
    """Service to interact with Dremio data lake for EEA data."""
    
    def __init__(self, 
                 username: Optional[str] = None,
                 password: Optional[str] = None, 
                 server: Optional[str] = None,
                 server_auth: Optional[str] = None,
                 ssl: Optional[bool] = None,
                 timeout: Optional[int] = None):
        """
        Initialize Dremio service connection.
        
        Args:
            username: Dremio username (defaults to DREMIO_USERNAME env var)
            password: Dremio password (defaults to DREMIO_PASSWORD env var)
            server: Dremio server URL (defaults to DREMIO_SERVER env var)
            server_auth: Dremio auth server URL (defaults to DREMIO_SERVER_AUTH env var)
            ssl: Whether to use SSL (defaults to DREMIO_SSL env var)
            timeout: Request timeout in milliseconds (defaults to DREMIO_TIMEOUT env var)
        """
        # Use provided values or fall back to environment variables
        self.username = username or os.getenv('DREMIO_USERNAME')
        self.password = password or os.getenv('DREMIO_PASSWORD')
        self.server = server or os.getenv('DREMIO_SERVER')
        self.server_auth = server_auth or os.getenv('DREMIO_SERVER_AUTH')
        
        # Handle SSL boolean from environment
        if ssl is not None:
            self.ssl = ssl
        else:
            ssl_env = os.getenv('DREMIO_SSL', 'false').lower()
            self.ssl = ssl_env in ('true', '1', 'yes', 'on')
        
        # Handle timeout from environment
        if timeout is not None:
            self.timeout = timeout / 1000  # Convert to seconds
        else:
            timeout_env = int(os.getenv('DREMIO_TIMEOUT', '60000'))
            self.timeout = timeout_env / 1000  # Convert to seconds
        
        # Validate required credentials
        if not self.username or not self.password:
            raise ValueError("Dremio username and password are required. Set DREMIO_USERNAME and DREMIO_PASSWORD environment variables.")
        
        if not self.server or not self.server_auth:
            raise ValueError("Dremio server URLs are required. Set DREMIO_SERVER and DREMIO_SERVER_AUTH environment variables.")
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EEA-Dremio-Client/1.0',
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'
        })

        # Configure session for better connection handling
        adapter = requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=10,
            pool_maxsize=10
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        # Disable SSL verification if ssl is False
        if not self.ssl:
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.token = None
        self._authenticate()
    
    def _authenticate(self) -> None:
        """Authenticate with Dremio and get access token."""
        auth_url = urljoin(self.server_auth, '/apiv2/login')
        
        auth_data = {
            "userName": self.username,
            "password": self.password
        }
        
        try:
            print(f"DEBUG: Authenticating with {auth_url}")
            response = self.session.post(
                auth_url,
                json=auth_data,
                timeout=self.timeout
            )

            print(f"DEBUG: Auth response status: {response.status_code}")
            response.raise_for_status()

            auth_result = response.json()
            self.token = auth_result.get('token')

            if self.token:
                self.session.headers.update({
                    'Authorization': f'_dremio{self.token}'
                })
                print("DEBUG: Authentication successful")
            else:
                raise Exception("No token received from authentication")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Authentication failed: {str(e)}")

    def get_service_info(self) -> Dict[str, Any]:
        """Get information about the Dremio service."""
        return {
            "configured_mode": "dremio",
            "active_service": "dremio",
            "service_class": self.__class__.__name__
        }

    def execute_query(self, 
                     sql_query: str, 
                     limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute SQL query against Dremio.
        
        Args:
            sql_query: SQL query to execute
            limit: Maximum number of rows to return
            
        Returns:
            Dictionary containing query results and metadata
        """
        # Don't wrap with nested SELECT - add LIMIT directly to the query
        if limit and not sql_query.upper().strip().endswith('LIMIT'):
            sql_query = f"{sql_query.rstrip()} LIMIT {limit}"
        
        # Debug: Print the final query being executed
        print(f"DEBUG: Final SQL query: {sql_query}")
        
        query_url = urljoin(self.server, '/apiv2/sql')
        
        query_data = {
            "sql": sql_query
        }
        
        try:
            # Use longer timeout for queries (3x the default timeout)
            query_timeout = self.timeout * 3
            print(f"DEBUG: Executing query with timeout: {query_timeout}s")

            response = self.session.post(
                query_url,
                json=query_data,
                timeout=query_timeout,
                stream=False
            )

            print(f"DEBUG: Response status: {response.status_code}")

            if not response.ok:
                print(f"DEBUG: Dremio error response: {response.status_code} - {response.text}")
                # Try to parse error message from response
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get('errorMessage', response.text)
                except:
                    error_msg = response.text
                raise Exception(f"Dremio API error {response.status_code}: {error_msg}")

            result = response.json()
            print(f"DEBUG: Query executed successfully, processing results...")

            return result

        except requests.exceptions.Timeout as e:
            raise Exception(f"Query execution timed out after {query_timeout}s: {str(e)}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error to Dremio server: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def query_to_dataframe(self, 
                          sql_query: str, 
                          limit: Optional[int] = None) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.
        
        Args:
            sql_query: SQL query to execute
            limit: Maximum number of rows to return
            
        Returns:
            pandas DataFrame containing query results
        """
        try:
            result = self.execute_query(sql_query, limit)
            
            if 'rows' in result and result['rows']:
                # Extract column names
                columns = [col['name'] for col in result.get('columns', [])]
                
                # Handle Dremio response format: rows are {'row': [data]} and values are {'v': actual_value}
                data_rows = []
                for row in result['rows']:
                    if isinstance(row, dict) and 'row' in row:
                        # Extract actual values from {'v': value} format
                        clean_row = []
                        for cell in row['row']:
                            if isinstance(cell, dict) and 'v' in cell:
                                clean_row.append(cell['v'])
                            else:
                                clean_row.append(cell)
                        data_rows.append(clean_row)
                    elif isinstance(row, list):
                        data_rows.append(row)
                    else:
                        # Fallback: assume row is already in correct format
                        data_rows.append(row)
                
                # Convert rows to DataFrame
                df = pd.DataFrame(data_rows, columns=columns)
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return pd.DataFrame()
    
    def get_waterbase_data(self,
                         country_code: Optional[str] = None,
                         limit: int = 1000,
                         include_coordinates: bool = False) -> Dict[str, Any]:
        """
        Get waterbase disaggregated data from Dremio with optional coordinate enrichment.

        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            Dictionary containing waterbase disaggregated data
        """
        if include_coordinates:
            # Optimized query with coordinate JOIN
            base_query = '''
            SELECT
                w.*,
                s.lat as coordinate_latitude,
                s.lon as coordinate_longitude,
                s.thematicIdIdentifier as coordinate_thematic_identifier,
                s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                s.monitoringSiteName as coordinate_site_name
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
            LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
                AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                AND s.lat IS NOT NULL
                AND s.lon IS NOT NULL
            '''
        else:
            base_query = 'SELECT * FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"'

        if country_code:
            # Normalize country code to uppercase and add debugging
            country_code = country_code.upper()
            query = f"""
            {base_query}
            WHERE {"w." if include_coordinates else ""}countryCode = '{country_code}'
            ORDER BY {"w." if include_coordinates else ""}phenomenonTimeSamplingDate DESC
            LIMIT {limit}
            """
            print(f"DEBUG: Executing disaggregated query with country filter: {country_code}")
        else:
            query = f"""
            {base_query}
            ORDER BY {"w." if include_coordinates else ""}phenomenonTimeSamplingDate DESC
            LIMIT {limit}
            """

        return self.execute_query(query, None)  # Don't pass limit to execute_query since it's in the query
    
    def get_waterbase_disaggregated_data(self,
                                       country_code: Optional[str] = None,
                                       limit: int = 1000) -> Dict[str, Any]:
        """
        Get waterbase disaggregated data from Dremio.
        (Alias for get_waterbase_data for backward compatibility)

        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return

        Returns:
            Dictionary containing waterbase disaggregated data
        """
        return self.get_waterbase_data(country_code, limit)
    
    def get_waterbase_dataframe(self,
                              country_code: Optional[str] = None,
                              limit: int = 1000,
                              include_coordinates: bool = False) -> pd.DataFrame:
        """
        Get waterbase disaggregated data as pandas DataFrame with optional coordinate enrichment.

        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            pandas DataFrame containing waterbase disaggregated data
        """
        try:
            result = self.get_waterbase_data(country_code, limit, include_coordinates)

            if 'rows' in result and result['rows']:
                columns = [col['name'] for col in result.get('columns', [])]

                # Handle Dremio response format: rows are {'row': [data]} and values are {'v': actual_value}
                data_rows = []
                for row in result['rows']:
                    if isinstance(row, dict) and 'row' in row:
                        # Extract actual values from {'v': value} format
                        clean_row = []
                        for cell in row['row']:
                            if isinstance(cell, dict) and 'v' in cell:
                                clean_row.append(cell['v'])
                            else:
                                clean_row.append(cell)
                        data_rows.append(clean_row)
                    elif isinstance(row, list):
                        data_rows.append(row)
                    else:
                        data_rows.append(row)

                return pd.DataFrame(data_rows, columns=columns)
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"Error getting waterbase data: {str(e)}")
            return pd.DataFrame()
    
    def get_waterbase_disaggregated_dataframe(self,
                                            country_code: Optional[str] = None,
                                            limit: int = 1000) -> pd.DataFrame:
        """
        Get waterbase disaggregated data as pandas DataFrame.
        (Alias for get_waterbase_dataframe for backward compatibility)

        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return

        Returns:
            pandas DataFrame containing waterbase disaggregated data
        """
        return self.get_waterbase_dataframe(country_code, limit)
    
    def get_latest_measurements_by_country(self, country_code: str, include_coordinates: bool = False) -> Dict[str, Any]:
        """
        Get the latest measurement for each chemical parameter by country with optional coordinates.

        Args:
            country_code: Country code (e.g., 'FR', 'DE')
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            Dictionary containing the latest measurements per parameter
        """
        if include_coordinates:
            query = f'''
            SELECT t1.*,
                   s.lat as coordinate_latitude,
                   s.lon as coordinate_longitude,
                   s.thematicIdIdentifier as coordinate_thematic_identifier,
                   s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                   s.monitoringSiteName as coordinate_site_name
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" t1
            INNER JOIN (
                SELECT observedPropertyDeterminandCode,
                       MAX(phenomenonTimeSamplingDate_year) as max_year
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
                WHERE countryCode = '{country_code}'
                GROUP BY observedPropertyDeterminandCode
            ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode
               AND t1.phenomenonTimeSamplingDate_year = t2.max_year
            LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                ON t1.monitoringSiteIdentifier = s.thematicIdIdentifier
                AND t1.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                AND s.lat IS NOT NULL
                AND s.lon IS NOT NULL
            WHERE t1.countryCode = '{country_code}'
            ORDER BY t1.phenomenonTimeSamplingDate DESC
            '''
        else:
            query = f'''
            SELECT t1.* FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" t1
            INNER JOIN (
                SELECT observedPropertyDeterminandCode,
                       MAX(phenomenonTimeSamplingDate_year) as max_year
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
                WHERE countryCode = '{country_code}'
                GROUP BY observedPropertyDeterminandCode
            ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode
               AND t1.phenomenonTimeSamplingDate_year = t2.max_year
            WHERE t1.countryCode = '{country_code}'
            ORDER BY t1.phenomenonTimeSamplingDate DESC
            '''

        print(f"DEBUG: Dremio latest by country query: {query}")
        return self.execute_query(query)
    
    def get_latest_measurements_by_site(self, site_identifier: str, include_coordinates: bool = False) -> Dict[str, Any]:
        """
        Get the latest measurement for each chemical parameter by monitoring site with optional coordinates.

        Args:
            site_identifier: Monitoring site identifier (e.g., 'FRFR05026000')
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            Dictionary containing the latest measurements per parameter for the site
        """
        if include_coordinates:
            query = f'''
            SELECT t1.*,
                   s.lat as coordinate_latitude,
                   s.lon as coordinate_longitude,
                   s.thematicIdIdentifier as coordinate_thematic_identifier,
                   s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                   s.monitoringSiteName as coordinate_site_name
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" t1
            INNER JOIN (
                SELECT observedPropertyDeterminandCode,
                       MAX(phenomenonTimeSamplingDate_year) as max_year
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
                WHERE monitoringSiteIdentifier = '{site_identifier}'
                GROUP BY observedPropertyDeterminandCode
            ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode
               AND t1.phenomenonTimeSamplingDate_year = t2.max_year
            LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                ON t1.monitoringSiteIdentifier = s.thematicIdIdentifier
                AND t1.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                AND s.lat IS NOT NULL
                AND s.lon IS NOT NULL
            WHERE t1.monitoringSiteIdentifier = '{site_identifier}'
            ORDER BY t1.phenomenonTimeSamplingDate DESC
            '''
        else:
            query = f'''
            SELECT t1.* FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" t1
            INNER JOIN (
                SELECT observedPropertyDeterminandCode,
                       MAX(phenomenonTimeSamplingDate_year) as max_year
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
                WHERE monitoringSiteIdentifier = '{site_identifier}'
                GROUP BY observedPropertyDeterminandCode
            ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode
               AND t1.phenomenonTimeSamplingDate_year = t2.max_year
            WHERE t1.monitoringSiteIdentifier = '{site_identifier}'
            ORDER BY t1.phenomenonTimeSamplingDate DESC
            '''

        print(f"DEBUG: Dremio latest by site query: {query}")
        return self.execute_query(query)

    def get_timeseries_by_site(self,
                              site_identifier: str,
                              parameter_code: Optional[str] = None,
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              interval: str = 'raw',
                              include_coordinates: bool = False) -> Dict[str, Any]:
        """
        Get time-series data for a specific monitoring site.

        Args:
            site_identifier: Monitoring site identifier
            parameter_code: Chemical parameter code filter (optional)
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            interval: Aggregation interval ('raw', 'monthly', 'yearly')
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            Dictionary containing time-series data
        """
        if interval == 'raw':
            # Raw data without aggregation
            if include_coordinates:
                base_query = '''
                SELECT w.*,
                       s.lat as coordinate_latitude,
                       s.lon as coordinate_longitude,
                       s.thematicIdIdentifier as coordinate_thematic_identifier,
                       s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                       s.monitoringSiteName as coordinate_site_name
                FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
                LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                    ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
                    AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                    AND s.lat IS NOT NULL
                    AND s.lon IS NOT NULL
                '''
            else:
                base_query = 'SELECT * FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"'

        elif interval == 'monthly':
            # Monthly aggregation
            coord_join = '''
            LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
                AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                AND s.lat IS NOT NULL
                AND s.lon IS NOT NULL
            ''' if include_coordinates else ''

            coord_select = '''
                s.lat as coordinate_latitude,
                s.lon as coordinate_longitude,
                s.thematicIdIdentifier as coordinate_thematic_identifier,
                s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                s.monitoringSiteName as coordinate_site_name,
            ''' if include_coordinates else ''

            base_query = f'''
            SELECT
                w.monitoringSiteIdentifier,
                w.monitoringSiteIdentifierScheme,
                w.countryCode,
                w.observedPropertyDeterminandCode,
                w.observedPropertyDeterminandLabel,
                w.resultUom,
                DATE_TRUNC('month', w.phenomenonTimeSamplingDate) as time_period,
                AVG(CAST(w.resultObservedValue AS DOUBLE)) as avg_value,
                MIN(CAST(w.resultObservedValue AS DOUBLE)) as min_value,
                MAX(CAST(w.resultObservedValue AS DOUBLE)) as max_value,
                COUNT(*) as sample_count,
                {coord_select}
                'monthly' as aggregation_interval
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
            {coord_join}
            '''

        elif interval == 'yearly':
            # Yearly aggregation
            coord_join = '''
            LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
                ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
                AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
                AND s.lat IS NOT NULL
                AND s.lon IS NOT NULL
            ''' if include_coordinates else ''

            coord_select = '''
                s.lat as coordinate_latitude,
                s.lon as coordinate_longitude,
                s.thematicIdIdentifier as coordinate_thematic_identifier,
                s.thematicIdIdentifierScheme as coordinate_thematic_scheme,
                s.monitoringSiteName as coordinate_site_name,
            ''' if include_coordinates else ''

            base_query = f'''
            SELECT
                w.monitoringSiteIdentifier,
                w.monitoringSiteIdentifierScheme,
                w.countryCode,
                w.observedPropertyDeterminandCode,
                w.observedPropertyDeterminandLabel,
                w.resultUom,
                w.phenomenonTimeSamplingDate_year as time_period,
                AVG(CAST(w.resultObservedValue AS DOUBLE)) as avg_value,
                MIN(CAST(w.resultObservedValue AS DOUBLE)) as min_value,
                MAX(CAST(w.resultObservedValue AS DOUBLE)) as max_value,
                COUNT(*) as sample_count,
                {coord_select}
                'yearly' as aggregation_interval
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
            {coord_join}
            '''
        else:
            raise ValueError(f"Unsupported interval: {interval}")

        # Build WHERE clause with proper table aliases
        if interval == 'raw' and include_coordinates:
            # For raw data with coordinates, use 'w.' prefix
            where_conditions = [f"w.monitoringSiteIdentifier = '{site_identifier}'"]

            if parameter_code:
                where_conditions.append(f"w.observedPropertyDeterminandCode = '{parameter_code}'")

            if start_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate >= '{start_date}'")

            if end_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate <= '{end_date}'")

        elif interval == 'raw':
            # For raw data without coordinates, no prefix needed
            where_conditions = [f"monitoringSiteIdentifier = '{site_identifier}'"]

            if parameter_code:
                where_conditions.append(f"observedPropertyDeterminandCode = '{parameter_code}'")

            if start_date:
                where_conditions.append(f"phenomenonTimeSamplingDate >= '{start_date}'")

            if end_date:
                where_conditions.append(f"phenomenonTimeSamplingDate <= '{end_date}'")
        else:
            # For aggregated data, use 'w.' prefix
            where_conditions = [f"w.monitoringSiteIdentifier = '{site_identifier}'"]

            if parameter_code:
                where_conditions.append(f"w.observedPropertyDeterminandCode = '{parameter_code}'")

            if start_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate >= '{start_date}'")

            if end_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate <= '{end_date}'")

        # Add WHERE clause
        query = f"{base_query} WHERE {' AND '.join(where_conditions)}"

        # Add GROUP BY for aggregated queries
        if interval == 'monthly':
            group_cols = [
                'w.monitoringSiteIdentifier', 'w.monitoringSiteIdentifierScheme', 'w.countryCode',
                'w.observedPropertyDeterminandCode', 'w.observedPropertyDeterminandLabel', 'w.resultUom',
                'DATE_TRUNC(\'month\', w.phenomenonTimeSamplingDate)'
            ]
            if include_coordinates:
                group_cols.extend([
                    's.lat', 's.lon', 's.thematicIdIdentifier',
                    's.thematicIdIdentifierScheme', 's.monitoringSiteName'
                ])
            query += f" GROUP BY {', '.join(group_cols)}"

        elif interval == 'yearly':
            group_cols = [
                'w.monitoringSiteIdentifier', 'w.monitoringSiteIdentifierScheme', 'w.countryCode',
                'w.observedPropertyDeterminandCode', 'w.observedPropertyDeterminandLabel', 'w.resultUom',
                'w.phenomenonTimeSamplingDate_year'
            ]
            if include_coordinates:
                group_cols.extend([
                    's.lat', 's.lon', 's.thematicIdIdentifier',
                    's.thematicIdIdentifierScheme', 's.monitoringSiteName'
                ])
            query += f" GROUP BY {', '.join(group_cols)}"

        # Order by time descending
        if interval == 'raw':
            if include_coordinates:
                query += " ORDER BY w.phenomenonTimeSamplingDate DESC"
            else:
                query += " ORDER BY phenomenonTimeSamplingDate DESC"
        else:
            query += " ORDER BY time_period DESC"

        query += " LIMIT 10000"  # Reasonable limit for time-series data

        print(f"DEBUG: Time-series query for site {site_identifier}: {query}")
        return self.execute_query(query)

    def get_available_parameters(self) -> Dict[str, Any]:
        """
        Get list of available chemical parameters with metadata.

        Returns:
            Dictionary containing available parameters
        """
        query = '''
        SELECT DISTINCT
            observedPropertyDeterminandCode,
            observedPropertyDeterminandLabel,
            resultUom,
            COUNT(*) as measurement_count
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
        GROUP BY observedPropertyDeterminandCode, observedPropertyDeterminandLabel, resultUom
        ORDER BY observedPropertyDeterminandLabel
        '''

        print("DEBUG: Getting available parameters")
        return self.execute_query(query)

    def get_available_sites(self, country_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Get list of available monitoring sites with coordinates.

        Args:
            country_code: Optional country filter

        Returns:
            Dictionary containing available sites
        """
        query = '''
        SELECT DISTINCT
            w.monitoringSiteIdentifier,
            w.monitoringSiteIdentifierScheme,
            w.countryCode,
            s.lat as latitude,
            s.lon as longitude,
            s.monitoringSiteName,
            COUNT(*) as measurement_count
        FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData" w
        LEFT JOIN "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData" s
            ON w.monitoringSiteIdentifier = s.thematicIdIdentifier
            AND w.monitoringSiteIdentifierScheme = s.thematicIdIdentifierScheme
        '''

        if country_code:
            query += f" WHERE w.countryCode = '{country_code.upper()}'"

        query += '''
        GROUP BY w.monitoringSiteIdentifier, w.monitoringSiteIdentifierScheme, w.countryCode,
                 s.lat, s.lon, s.monitoringSiteName
        ORDER BY w.countryCode, w.monitoringSiteIdentifier
        LIMIT 1000
        '''

        print(f"DEBUG: Getting available sites{' for ' + country_code if country_code else ''}")
        return self.execute_query(query)


    def close(self) -> None:
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()