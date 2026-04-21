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
            self.owner_name = os.getenv('MIDDLEWARE_OWNER_NAME', 'WISE_SOE')
            self._view_id_cache = {}  # path -> view _id cache
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
        Execute SQL query through middleware SQL endpoint.

        Args:
            sql_query: SQL query to execute

        Returns:
            Dictionary containing query results

        Raises:
            Exception: If query execution fails
        """
        query_url = urljoin(self.middleware_url, '/api/Dremio/ExecuteRawQuery')
        query_data = {"query": sql_query}

        try:
            # Use longer timeout for queries (3x the default timeout)
            query_timeout = self.timeout * 3
            print(f"DEBUG: Executing MIDDLEWARE SQL query to {query_url} with timeout: {query_timeout}s")

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

    def _resolve_view_id(self, view_path: str) -> str:
        """Resolve a view path to its MongoDB id via the middleware API, with caching.

        view_path is a dot-separated path; the last segment is matched against view names.
        e.g. 'discoData.gold.WISE_SOE.latest.Waterbase_V_MonitoringSites' → 'Waterbase_V_MonitoringSites'
        """
        if view_path in self._view_id_cache:
            return self._view_id_cache[view_path]

        view_name = view_path.split('.')[-1]

        headers = {
            'User-Agent': 'EEA-Dremio-Client/1.0',
            'Accept': 'application/json'
        }

        # Step 1: get owner ID — response is a list of owner objects
        owners_url = f"{self.middleware_url}/api/data-products/owners"
        resp = requests.get(owners_url, headers=headers, verify=self.ssl, timeout=self.timeout)
        resp.raise_for_status()
        owners = resp.json()
        # Handle both list and single-object responses
        if isinstance(owners, dict):
            owners = [owners]
        owner_id = None
        for owner in owners:
            if owner.get('owner') == self.owner_name or owner.get('name') == self.owner_name:
                owner_id = str(owner.get('id') or owner.get('_id'))
                break
        if not owner_id:
            raise Exception(f"Owner '{self.owner_name}' not found. Available: {[o.get('owner') or o.get('name') for o in owners]}")

        # Step 2: get views for that owner — response is {"id":..., "owner":..., "views":[...]}
        views_url = f"{self.middleware_url}/api/data-products/owners/{owner_id}/views"
        resp = requests.get(views_url, headers=headers, verify=self.ssl, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        views = data.get('views', []) if isinstance(data, dict) else data

        # Cache all views by name so future calls don't re-fetch
        for view in views:
            name = view.get('name')
            vid = str(view.get('id') or view.get('_id'))
            if name:
                self._view_id_cache[name] = vid

        if view_name not in self._view_id_cache:
            available = [v.get('name') for v in views]
            raise Exception(f"View '{view_name}' not found. Available: {available}")

        # Also cache by full path for future lookups
        self._view_id_cache[view_path] = self._view_id_cache[view_name]
        print(f"DEBUG: Resolved view '{view_name}' → id={self._view_id_cache[view_path]}")
        return self._view_id_cache[view_path]

    def execute_view_query(self,
                           view_path: str,
                           fields: list,
                           filters: list = None,
                           limit: Optional[int] = None,
                           offset: Optional[int] = None,
                           aggregates: list = None,
                           group_by: list = None) -> Dict[str, Any]:
        """
        Execute a structured query against a Dremio view via the middleware
        data-product endpoint.

        Args:
            view_path: Dot-separated view path
            fields: List of field names to select
            filters: List of filter dicts with keys: fieldName, condition, values, concat
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            aggregates: List of aggregate dicts with keys: function, field, alias (and granularity for DATE_TRUNC)
            group_by: List of field names to group by

        Returns:
            List or dictionary containing query results
        """
        view_id = self._resolve_view_id(view_path)
        query_url = f"{self.middleware_url}/api/data-products/views/{view_id}/data"

        payload = {
            "fields": fields,
            "filters": filters or []
        }
        if limit is not None:
            payload["limit"] = limit
        if offset is not None:
            payload["offset"] = offset
        if aggregates:
            payload["aggregates"] = aggregates
        if group_by:
            payload["groupBy"] = group_by

        try:
            query_timeout = self.timeout * 3
            print(f"DEBUG: Executing VIEW query to {query_url}")
            print(f"DEBUG: Payload: {payload}")

            response = requests.post(
                query_url,
                json=payload,
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
                print(f"DEBUG: View query error: {response.status_code} - {response.text}")
                try:
                    error_detail = response.json()
                    error_msg = error_detail.get('errorMessage', response.text)
                except Exception:
                    error_msg = response.text
                raise Exception(f"View query API error {response.status_code}: {error_msg}")

            result = response.json()
            print(f"DEBUG: View query response type: {type(result)}")
            if isinstance(result, dict):
                print(f"DEBUG: View query response keys: {list(result.keys())}")
                print(f"DEBUG: View query returned {len(result.get('rows', []))} rows")
            elif isinstance(result, list):
                print(f"DEBUG: View query returned list with {len(result)} items")
                if result:
                    print(f"DEBUG: First item keys: {list(result[0].keys()) if isinstance(result[0], dict) else result[0]}")

            return result

        except requests.exceptions.Timeout as e:
            raise Exception(f"View query timed out: {str(e)}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error to middleware: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"View query failed: {str(e)}")

    def get_timeseries_by_site(self,
                              site_identifier: str,
                              parameter_code: Optional[str] = None,
                              start_date: Optional[str] = None,
                              end_date: Optional[str] = None,
                              interval: str = 'raw',
                              **kwargs) -> list:
        """
        Get time-series data for a specific monitoring site with optional aggregation.

        Uses the middleware view query endpoint for all intervals.
        Raw: direct query on the view.
        Monthly/Yearly: uses aggregates and groupBy via the middleware.

        Args:
            site_identifier: Monitoring site identifier
            parameter_code: Chemical parameter code filter (optional)
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            interval: Aggregation interval ('raw', 'monthly', 'yearly')
            include_coordinates: Whether to include GPS coordinates via JOIN

        Returns:
            List of dictionaries containing time-series data
        """
        VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_Timeseries"

        # Build filters (same for all intervals)
        filters = [
            {"fieldName": "monitoringSiteIdentifier", "condition": "=", "values": [site_identifier], "concat": "AND"}
        ]

        if parameter_code:
            filters.append({"fieldName": "observedPropertyDeterminandCode", "condition": "=", "values": [parameter_code], "concat": "OR"})
            filters.append({"fieldName": "observedPropertyDeterminandLabel", "condition": "=", "values": [parameter_code], "concat": "AND"})

        if start_date:
            # Support year-only input (e.g., '2020' -> '2020-01-01')
            if len(start_date) == 4:
                start_date = f"{start_date}-01-01"
            filters.append({"fieldName": "phenomenonTimeSamplingDate", "condition": ">=", "values": [start_date], "concat": "AND"})

        if end_date:
            # Support year-only input (e.g., '2023' -> '2023-12-31')
            if len(end_date) == 4:
                end_date = f"{end_date}-12-31"
            filters.append({"fieldName": "phenomenonTimeSamplingDate", "condition": "<=", "values": [end_date], "concat": "AND"})

        if interval == 'raw':
            fields = [
                "monitoringSiteIdentifier", "monitoringSiteIdentifierScheme", "countryCode",
                "observedPropertyDeterminandCode", "observedPropertyDeterminandLabel", "resultUom",
                "phenomenonTimeSamplingDate", "resultObservedValue",
                "lat", "lon", "thematicIdIdentifier", "thematicIdIdentifierScheme", "monitoringSiteName"
            ]

            print(f"DEBUG: Time-series raw query for site {site_identifier}")
            result = self.execute_view_query(VIEW_PATH, fields, filters, limit=50000)

        elif interval in ('monthly', 'yearly'):
            group_fields = [
                "monitoringSiteIdentifier", "monitoringSiteIdentifierScheme", "countryCode",
                "observedPropertyDeterminandCode", "observedPropertyDeterminandLabel", "resultUom",
                "lat", "lon", "monitoringSiteName"
            ]

            aggregates = [
                {"function": "AVG", "field": "resultObservedValue", "alias": "avg_value"},
                {"function": "MIN", "field": "resultObservedValue", "alias": "min_value"},
                {"function": "MAX", "field": "resultObservedValue", "alias": "max_value"},
                {"function": "COUNT", "field": "*", "alias": "sample_count"}
            ]

            if interval == 'monthly':
                aggregates.append({"function": "DATE_TRUNC", "field": "phenomenonTimeSamplingDate", "granularity": "month", "alias": "time_period"})
            else:
                aggregates.append({"function": "DATE_TRUNC", "field": "phenomenonTimeSamplingDate", "granularity": "year", "alias": "time_period"})

            group_by = group_fields + ["time_period"]

            print(f"DEBUG: Time-series {interval} query for site {site_identifier}")
            result = self.execute_view_query(
                VIEW_PATH, group_fields, filters, limit=50000,
                aggregates=aggregates, group_by=group_by
            )
        else:
            raise ValueError(f"Unsupported interval: {interval}")

        # Normalize result to list
        data = result if isinstance(result, list) else []

        # Rename coordinate fields to match expected format
        for item in data:
            item['coordinate_latitude'] = item.pop('lat', None)
            item['coordinate_longitude'] = item.pop('lon', None)
            item['coordinate_thematic_identifier'] = item.pop('thematicIdIdentifier', None)
            item['coordinate_thematic_scheme'] = item.pop('thematicIdIdentifierScheme', None)
            item['coordinate_site_name'] = item.pop('monitoringSiteName', None)

        return data

    def get_available_parameters(self) -> list:
        """
        Get list of available chemical parameters with metadata.

        Used by: /parameters endpoint1

        Returns:
            List of dictionaries containing available parameters with measurement counts
        """
        VIEW_PATH = "discoData.gold.WISE_SOE.latest.Waterbase_V_Parameters"

        fields = [
            "observedPropertyDeterminandCode",
            "observedPropertyDeterminandLabel",
            "resultUom",
            "measurement_count"
        ]

        print("DEBUG: Getting available parameters")
        result = self.execute_view_query(VIEW_PATH, fields)
        return result if isinstance(result, list) else []

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
