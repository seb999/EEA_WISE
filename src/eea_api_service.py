import requests
import pandas as pd
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlencode
import json
import sqlite3
from pathlib import Path


class EEAApiService:
    """Service to interact with the European Environment Agency (EEA) DiscoData SQL API."""
    
    BASE_URL = "https://discodata.eea.europa.eu/sql"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EEA-Data-Client/1.0'
        })
        self.coordinates_db_path = Path(__file__).parent / "coordinates.db"
        self._coordinates_cache = {}  # In-memory cache for performance
    
    def execute_query(
        self, 
        query: str, 
        page: int = 1, 
        hits_per_page: int = 50,
        mail: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query against the EEA DiscoData API.
        
        Args:
            query: SQL query to execute
            page: Page number (default: 1)
            hits_per_page: Number of records per page (default: 50)
            mail: Email address (optional)
            schema: Schema name (optional)
            
        Returns:
            Dictionary containing the API response
            
        Raises:
            requests.RequestException: If the API request fails
        """
        params = {
            'query': query,
            'p': page,
            'nrOfHits': hits_per_page
        }
        
        if mail:
            params['mail'] = mail
        if schema:
            params['schema'] = schema
        else:
            params['schema'] = 'null'
            
        if not mail:
            params['mail'] = 'null'
        
        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from EEA API: {e}")
    
    def get_waterbase_aggregated_data(
        self, 
        limit: int = 1000, 
        page: int = 1,
        hits_per_page: int = 50
    ) -> Dict[str, Any]:
        """
        Get waterbase aggregated data from the WISE_SOE database.
        
        Args:
            limit: Maximum number of records to fetch
            page: Page number
            hits_per_page: Number of records per page
            
        Returns:
            Dictionary containing the waterbase aggregated data
        """
        query = f"SELECT TOP {limit} * FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] order by phenomenonTimeReferenceYear desc"
        return self.execute_query(query, page, hits_per_page)
    
    def query_to_dataframe(
        self, 
        query: str, 
        page: int = 1,
        hits_per_page: int = 50
    ) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query to execute
            page: Page number
            hits_per_page: Number of records per page
            
        Returns:
            pandas DataFrame containing the query results
        """
        response = self.execute_query(query, page, hits_per_page)
        
        if 'results' in response and response['results']:
            return pd.DataFrame(response['results'])
        else:
            return pd.DataFrame()
    
    def get_waterbase_aggregated_dataframe(
        self, 
        limit: int = 100,
        page: int = 1,
        hits_per_page: int = 50
    ) -> pd.DataFrame:
        """
        Get waterbase aggregated data as a pandas DataFrame.
        
        Args:
            limit: Maximum number of records to fetch
            page: Page number
            hits_per_page: Number of records per page
            
        Returns:
            pandas DataFrame containing waterbase aggregated data
        """
        query = f"SELECT TOP {limit} * FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] order by phenomenonTimeReferenceYear desc"
        return self.query_to_dataframe(query, page, hits_per_page)
    
    def get_available_countries(self) -> Dict[str, Any]:
        """
        Get list of available countries in the waterbase database.
        
        Returns:
            Dictionary containing the available countries
        """
        query = """
        SELECT DISTINCT countryCode
        FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] 
        ORDER BY countryCode
        """
        return self.execute_query(query, page=1, hits_per_page=100)
    
    def get_available_parameters(self, country: Optional[str] = None) -> Dict[str, Any]:
        """
        Get list of water parameters, optionally filtered by country.
        
        Args:
            country: Optional country code filter
            
        Returns:
            Dictionary containing the available parameters
        """
        if country:
            country = country.upper()
            query = f"""
            SELECT DISTINCT parameterWaterCategoryCode
            FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] 
            WHERE countryCode = '{country}'
            ORDER BY parameterWaterCategoryCode
            """
        else:
            query = """
            SELECT DISTINCT parameterWaterCategoryCode
            FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] 
            ORDER BY parameterWaterCategoryCode
            """
        return self.execute_query(query, page=1, hits_per_page=100)
    
    def get_waterbase_summary_dataframe(self, country: Optional[str] = None) -> pd.DataFrame:
        """
        Get waterbase data for summary analysis as a pandas DataFrame.
        
        Args:
            country: Optional country code filter
            
        Returns:
            pandas DataFrame containing waterbase data for analysis
        """
        if country:
            country = country.upper()
            query = f"""
            SELECT TOP 1000 * 
            FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] 
            WHERE countryCode = '{country}'
            """
        else:
            query = "SELECT TOP 1000 * FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData]"
        
        return self.query_to_dataframe(query, page=1, hits_per_page=100)
    
    def get_coordinates(self, gwb_code: str) -> Optional[Tuple[float, float]]:
        """
        Get GPS coordinates for a groundwater body code.
        
        Args:
            gwb_code: Groundwater body code
            
        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        # Check in-memory cache first
        if gwb_code in self._coordinates_cache:
            return self._coordinates_cache[gwb_code]
        
        # Check if database exists
        if not self.coordinates_db_path.exists():
            return None
        
        try:
            conn = sqlite3.connect(self.coordinates_db_path)
            cursor = conn.cursor()
            
            result = cursor.execute(
                'SELECT latitude, longitude FROM coordinates WHERE gwb_code = ?',
                (gwb_code,)
            ).fetchone()
            
            conn.close()
            
            if result:
                coords = (float(result[0]), float(result[1]))
                # Cache for future use
                self._coordinates_cache[gwb_code] = coords
                return coords
                
        except Exception as e:
            # Silently handle database errors
            pass
            
        return None
    
    def enrich_with_coordinates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich groundwater body data with GPS coordinates.
        
        Args:
            data: Data dictionary containing groundwater body information
            
        Returns:
            Enhanced data dictionary with coordinates
        """
        # Look for groundwater body code in various possible fields
        gwb_code = (
            data.get('euGroundWaterBodyCode') or 
            data.get('groundwater_body_code') or
            data.get('gwb_code')
        )
        
        if gwb_code:
            coords = self.get_coordinates(gwb_code)
            if coords:
                data['coordinates'] = {
                    'latitude': coords[0],
                    'longitude': coords[1]
                }
                
        return data
    
    def enrich_results_with_coordinates(self, results: list) -> list:
        """
        Enrich a list of results with GPS coordinates.
        
        Args:
            results: List of data dictionaries
            
        Returns:
            List of enhanced data dictionaries
        """
        return [self.enrich_with_coordinates(result) for result in results]