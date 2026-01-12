import requests
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv
from urllib.parse import urljoin
import uuid

load_dotenv()

class DremioApiService:
    """
    Service to interact with Dremio data lake for EEA water quality data.

    This service supports two modes:
    - Direct mode: Connect directly to Dremio data lake
    - Middleware mode: Route queries through EEA middleware API

    Mode is controlled by API_MODE environment variable ('dremio' or 'middleware').
    """

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
        # Determine API mode
        self.api_mode = os.getenv('API_MODE', 'dremio').lower()

        # Middleware configuration
        if self.api_mode == 'middleware':
            self.middleware_url = os.getenv('EEA_MIDDLEWARE_BASE_URL')
            if not self.middleware_url:
                raise ValueError("EEA_MIDDLEWARE_BASE_URL is required when API_MODE=middleware")

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

            # Configure session for middleware
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'EEA-Dremio-Client/1.0',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })

            # Disable SSL verification if ssl is False (no retries for middleware to avoid Windows SSL issues)
            if not self.ssl:
                self.session.verify = False
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            self.token = None
            print(f"DEBUG: Initialized in MIDDLEWARE mode, endpoint: {self.middleware_url}")

        else:  # Direct Dremio mode
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
            print(f"DEBUG: Initialized in DIRECT mode, server: {self.server}")

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
        """
        Get information about the Dremio service.

        Returns:
            Dictionary with service information
        """
        if self.api_mode == 'middleware':
            return {
                "configured_mode": "middleware",
                "active_service": "middleware",
                "middleware_url": self.middleware_url,
                "service_class": self.__class__.__name__
            }
        else:
            return {
                "configured_mode": "dremio",
                "active_service": "dremio",
                "dremio_server": self.server,
                "service_class": self.__class__.__name__
            }

    def execute_query(self,
                     sql_query: str,
                     limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute SQL query against Dremio.

        This is the core method used by all API endpoints to query the Dremio data lake.

        Args:
            sql_query: SQL query to execute
            limit: Maximum number of rows to return (appended to query if provided)

        Returns:
            Dictionary containing query results with 'rows' and 'columns' keys

        Raises:
            Exception: If query execution fails
        """
        # Add LIMIT to query if specified and not already present
        if limit and not sql_query.upper().strip().endswith('LIMIT'):
            sql_query = f"{sql_query.rstrip()} LIMIT {limit}"

        print(f"DEBUG: Final SQL query: {sql_query}")

        # Route to appropriate implementation based on API mode
        if self.api_mode == 'middleware':
            return self._execute_query_middleware(sql_query)
        else:
            return self._execute_query_direct(sql_query)

    def _execute_query_direct(self, sql_query: str) -> Dict[str, Any]:
        """
        Execute query directly against Dremio.

        Args:
            sql_query: SQL query to execute

        Returns:
            Dictionary containing query results

        Raises:
            Exception: If query execution fails
        """
        query_url = urljoin(self.server, '/apiv2/sql')
        query_data = {"sql": sql_query}

        try:
            # Use longer timeout for queries (3x the default timeout)
            query_timeout = self.timeout * 3
            print(f"DEBUG: Executing DIRECT query with timeout: {query_timeout}s")

            response = self.session.post(
                query_url,
                json=query_data,
                timeout=query_timeout,
                stream=False
            )

            print(f"DEBUG: Response status: {response.status_code}")

            if not response.ok:
                print(f"DEBUG: Dremio error response: {response.status_code} - {response.text}")
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

    def _execute_query_middleware(self, sql_query: str) -> Dict[str, Any]:
        """
        Execute query through middleware API.

        Args:
            sql_query: SQL query to execute

        Returns:
            Dictionary containing query results

        Raises:
            Exception: If query execution fails
        """
        query_url = urljoin(self.middleware_url, '/api/Dremio/query-execution')
        query_data = {"query": sql_query}

        try:
            # Use longer timeout for queries (3x the default timeout)
            query_timeout = self.timeout * 3
            print(f"DEBUG: Executing MIDDLEWARE query to {query_url} with timeout: {query_timeout}s")
            print(f"DEBUG: SSL verification: {self.ssl}")

            # Use direct requests.post instead of session to avoid Windows SSL issues
            response = requests.post(
                query_url,
                json=query_data,
                headers={
                    'User-Agent': 'EEA-Dremio-Client/1.0',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                timeout=query_timeout,
                verify=self.ssl
            )

            print(f"DEBUG: Response status: {response.status_code}")

            if not response.ok:
                print(f"DEBUG: Middleware error response: {response.status_code} - {response.text}")
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get('errorMessage', response.text)
                except:
                    error_msg = response.text
                raise Exception(f"Middleware API error {response.status_code}: {error_msg}")

            # Parse JSON response - WiseQuery endpoint returns Dremio-compatible format
            result = response.json()
            print(f"DEBUG: Query executed successfully through middleware")
            print(f"DEBUG: Result has {len(result.get('rows', []))} rows and {len(result.get('columns', []))} columns")

            return result

        except requests.exceptions.Timeout as e:
            raise Exception(f"Query execution timed out after {query_timeout}s: {str(e)}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error to middleware server: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Query execution failed: {str(e)}")

    def get_timeseries_by_site(self,
                              site_identifier: str,
                              parameter_code: Optional[str] = None,
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              interval: str = 'raw',
                              include_coordinates: bool = False) -> Dict[str, Any]:
        """
        Get time-series data for a specific monitoring site with optional aggregation.

        Used by: /timeseries/site/{site_identifier} endpoint

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
            where_conditions = [f"w.monitoringSiteIdentifier = '{site_identifier}'"]
            if parameter_code:
                where_conditions.append(f"w.observedPropertyDeterminandCode = '{parameter_code}'")
            if start_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate >= '{start_date}'")
            if end_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate <= '{end_date}'")
        elif interval == 'raw':
            where_conditions = [f"monitoringSiteIdentifier = '{site_identifier}'"]
            if parameter_code:
                where_conditions.append(f"observedPropertyDeterminandCode = '{parameter_code}'")
            if start_date:
                where_conditions.append(f"phenomenonTimeSamplingDate >= '{start_date}'")
            if end_date:
                where_conditions.append(f"phenomenonTimeSamplingDate <= '{end_date}'")
        else:
            where_conditions = [f"w.monitoringSiteIdentifier = '{site_identifier}'"]
            if parameter_code:
                where_conditions.append(f"w.observedPropertyDeterminandCode = '{parameter_code}'")
            if start_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate >= '{start_date}'")
            if end_date:
                where_conditions.append(f"w.phenomenonTimeSamplingDate <= '{end_date}'")

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
            query += " ORDER BY w.phenomenonTimeSamplingDate DESC" if include_coordinates else " ORDER BY phenomenonTimeSamplingDate DESC"
        else:
            query += " ORDER BY time_period DESC"

        query += " LIMIT 10000"  # Reasonable limit for time-series data

        print(f"DEBUG: Time-series query for site {site_identifier}: {query}")
        return self.execute_query(query)

    def get_available_parameters(self) -> Dict[str, Any]:
        """
        Get list of available chemical parameters with metadata.

        Used by: /parameters endpoint

        Returns:
            Dictionary containing available parameters with measurement counts
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

    def close(self) -> None:
        """Close the session."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False  # Don't suppress exceptions
