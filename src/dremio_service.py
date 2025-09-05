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
            'Content-Type': 'application/json'
        })
        
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
            response = self.session.post(
                auth_url, 
                json=auth_data, 
                timeout=self.timeout
            )
            response.raise_for_status()
            
            auth_result = response.json()
            self.token = auth_result.get('token')
            
            if self.token:
                self.session.headers.update({
                    'Authorization': f'_dremio{self.token}'
                })
            else:
                raise Exception("No token received from authentication")
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"Authentication failed: {str(e)}")
    
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
        
        query_url = urljoin(self.server_auth, '/apiv2/sql')
        
        query_data = {
            "sql": sql_query
        }
        
        try:
            response = self.session.post(
                query_url,
                json=query_data,
                timeout=self.timeout
            )
            if not response.ok:
                print(f"DEBUG: Dremio error response: {response.status_code} - {response.text}")
            response.raise_for_status()
            result = response.json()
            
            # Debug: Print number of rows returned and first few country codes
            # if 'rows' in result and result['rows']:
            #     print(f"DEBUG: Query returned {len(result['rows'])} rows")
            #     # Try to find country codes in the first few rows
            #     for i, row in enumerate(result['rows'][:3]):
            #         if isinstance(row, list) and len(row) > 0:
            #             print(f"DEBUG: Row {i}: {row[:5]}...")  # Print first 5 columns
            #         elif isinstance(row, dict):
            #             country_val = row.get('countryCode') or row.get('CountryCode') or row.get('COUNTRYCODE')
            #             print(f"DEBUG: Row {i} countryCode: {country_val}")
            
            return result
            
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
    
    def get_waterbase_aggregated_data(self, 
                                    country_code: Optional[str] = None,
                                    limit: int = 1000) -> Dict[str, Any]:
        """
        Get waterbase aggregated data from Dremio.
        
        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            
        Returns:
            Dictionary containing waterbase data
        """
        base_query = 'SELECT * FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData"'
        
        if country_code:
            # Normalize country code to uppercase and add debugging
            country_code = country_code.upper()
            query = f"""
            {base_query}
            WHERE countryCode = '{country_code}'
            ORDER BY phenomenonTimeReferenceYear DESC
            LIMIT {limit}
            """
            print(f"DEBUG: Executing aggregated query with country filter: {country_code}")
        else:
            query = f"""
            {base_query}
            ORDER BY phenomenonTimeReferenceYear DESC
            LIMIT {limit}
            """
        
        return self.execute_query(query, None)  # Don't pass limit to execute_query since it's in the query
    
    def get_waterbase_disaggregated_data(self, 
                                       country_code: Optional[str] = None,
                                       limit: int = 1000) -> Dict[str, Any]:
        """
        Get waterbase disaggregated data from Dremio.
        
        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            
        Returns:
            Dictionary containing waterbase disaggregated data
        """
        base_query = 'SELECT * FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"'
        
        if country_code:
            # Normalize country code to uppercase and add debugging
            country_code = country_code.upper()
            query = f"""
            {base_query}
            WHERE countryCode = '{country_code}'
            LIMIT {limit}
            """
            print(f"DEBUG: Executing disaggregated query with country filter: {country_code}")
        else:
            query = f"""
            {base_query}
            LIMIT {limit}
            """
        
        return self.execute_query(query, None)  # Don't pass limit to execute_query since it's in the query
    
    def get_waterbase_aggregated_dataframe(self,
                                         country_code: Optional[str] = None,
                                         limit: int = 1000) -> pd.DataFrame:
        """
        Get waterbase aggregated data as pandas DataFrame.
        
        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            
        Returns:
            pandas DataFrame containing waterbase data
        """
        try:
            result = self.get_waterbase_aggregated_data(country_code, limit)
            
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
        
        Args:
            country_code: ISO country code filter (optional)
            limit: Maximum number of records to return
            
        Returns:
            pandas DataFrame containing waterbase disaggregated data
        """
        try:
            result = self.get_waterbase_disaggregated_data(country_code, limit)
            
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
            print(f"Error getting waterbase disaggregated data: {str(e)}")
            return pd.DataFrame()
    
    def get_latest_measurements_by_country(self, country_code: str) -> Dict[str, Any]:
        """
        Get the latest measurement for each chemical parameter by country.
        
        Args:
            country_code: Country code (e.g., 'FR', 'DE')
            
        Returns:
            Dictionary containing the latest measurements per parameter
        """
        query = f'''
        SELECT t1.* FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData" t1
        INNER JOIN (
            SELECT observedPropertyDeterminandCode, 
                   MAX(phenomenonTimeReferenceYear) as max_year
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData"
            WHERE countryCode = '{country_code}'
            GROUP BY observedPropertyDeterminandCode
        ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode 
           AND t1.phenomenonTimeReferenceYear = t2.max_year
        WHERE t1.countryCode = '{country_code}'
        ORDER BY t1.observedPropertyDeterminandLabel
        '''
        
        print(f"DEBUG: Dremio latest by country query: {query}")
        return self.execute_query(query)
    
    def get_latest_measurements_by_site(self, site_identifier: str) -> Dict[str, Any]:
        """
        Get the latest measurement for each chemical parameter by monitoring site.
        
        Args:
            site_identifier: Monitoring site identifier (e.g., 'FRFR05026000')
            
        Returns:
            Dictionary containing the latest measurements per parameter for the site
        """
        query = f'''
        SELECT t1.* FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData" t1
        INNER JOIN (
            SELECT observedPropertyDeterminandCode, 
                   MAX(phenomenonTimeReferenceYear) as max_year
            FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData"
            WHERE monitoringSiteIdentifier = '{site_identifier}'
            GROUP BY observedPropertyDeterminandCode
        ) t2 ON t1.observedPropertyDeterminandCode = t2.observedPropertyDeterminandCode 
           AND t1.phenomenonTimeReferenceYear = t2.max_year
        WHERE t1.monitoringSiteIdentifier = '{site_identifier}'
        ORDER BY t1.observedPropertyDeterminandLabel
        '''
        
        print(f"DEBUG: Dremio latest by site query: {query}")
        return self.execute_query(query)
    
    def close(self) -> None:
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()