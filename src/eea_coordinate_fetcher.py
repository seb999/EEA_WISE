"""
EEA Coordinate Fetcher

This script fetches monitoring site coordinates from official EEA data sources
and updates the local coordinate database with more comprehensive data.

Data sources:
1. EEA ESRI REST API - EIONET Monitoring Sites
2. EEA WISE WFD Spatial Data
"""

import requests
import sqlite3
import json
import logging
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EEACoordinateFetcher:
    """Fetch monitoring site coordinates from EEA official data sources."""
    
    def __init__(self, db_path: str = None):
        """Initialize the coordinate fetcher."""
        if db_path is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(current_dir, 'coordinates.db')
        
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EEA-WISE-API-Client/1.0'
        })
        
        # EEA API endpoints
        self.eionet_api = "https://water.discomap.eea.europa.eu/arcgis/rest/services/WISE_SoE/EIONET_MonitoringSite_WM/MapServer"
        self.wise_wfd_api = "https://water.discomap.eea.europa.eu/arcgis/rest/services/WISE_WFD/WFD_MonitoringSites_2016_WM/MapServer"
        self.wfd_2022_api = "https://water.discomap.eea.europa.eu/arcgis/rest/services/WISE_WFD/WFD2022_MonitoringSite_WM/MapServer"
    
    def test_api_endpoints(self) -> Dict[str, bool]:
        """Test if EEA API endpoints are accessible."""
        endpoints = {
            'eionet': f"{self.eionet_api}?f=json",
            'wise_wfd': f"{self.wise_wfd_api}?f=json",
            'wfd_2022': f"{self.wfd_2022_api}?f=json"
        }
        
        results = {}
        for name, url in endpoints.items():
            try:
                response = self.session.get(url, timeout=10)
                results[name] = response.status_code == 200
                if results[name]:
                    data = response.json()
                    logger.info(f"✓ {name} API accessible: {data.get('serviceDescription', 'No description')}")
                else:
                    logger.warning(f"✗ {name} API returned status {response.status_code}")
            except Exception as e:
                logger.error(f"✗ {name} API failed: {e}")
                results[name] = False
                
        return results
    
    def query_eionet_sites(self, where_clause: str = "1=1", limit: int = 1000) -> List[Dict]:
        """
        Query EIONET monitoring sites API.
        
        Args:
            where_clause: SQL WHERE clause for filtering
            limit: Maximum number of records to return
            
        Returns:
            List of monitoring site records with coordinates
        """
        try:
            # Query the feature layer (assuming layer 0)
            query_url = f"{self.eionet_api}/0/query"
            
            params = {
                'where': where_clause,
                'outFields': '*',  # Get all fields
                'returnGeometry': 'true',
                'geometryType': 'esriGeometryPoint',
                'spatialRel': 'esriSpatialRelIntersects',
                'f': 'json',
                'resultRecordCount': limit
            }
            
            logger.info(f"Querying EIONET API: {where_clause}")
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' not in data:
                logger.warning("No 'features' in API response")
                return []
            
            sites = []
            for feature in data['features']:
                attrs = feature.get('attributes', {})
                geom = feature.get('geometry', {})
                
                # Use lat/lon fields from attributes if available, otherwise use geometry
                lat = attrs.get('lat') or geom.get('y') 
                lon = attrs.get('lon') or geom.get('x')
                
                if lat is not None and lon is not None:
                    site = {
                        'site_id': (attrs.get('thematicIdIdentifier') or 
                                   attrs.get('inspireIdLocalId') or 
                                   attrs.get('monitoringSiteIdentifier') or 
                                   attrs.get('SITE_ID')),
                        'site_name': (attrs.get('nameTextInternational') or 
                                     attrs.get('nameText') or 
                                     attrs.get('siteName') or 
                                     attrs.get('SITE_NAME')),
                        'country_code': (attrs.get('countryCode') or 
                                       attrs.get('COUNTRY_CODE') or 
                                       attrs.get('COUNTRY')),
                        'longitude': lon,
                        'latitude': lat,
                        'water_category': attrs.get('WATER_CATEGORY') or attrs.get('waterCategory'),
                        'data_source': 'eea_eionet_monitoring_sites',
                        'confidence': 1.0,
                        'layer': 'EIONET_MonitoringSite'
                    }
                    sites.append(site)
            
            logger.info(f"Retrieved {len(sites)} sites from EIONET API")
            return sites
            
        except Exception as e:
            logger.error(f"Error querying EIONET sites: {e}")
            return []
    
    def query_wise_wfd_sites(self, where_clause: str = "1=1", limit: int = 1000) -> List[Dict]:
        """
        Query WISE WFD monitoring sites API.
        
        Args:
            where_clause: SQL WHERE clause for filtering  
            limit: Maximum number of records to return
            
        Returns:
            List of monitoring site records with coordinates
        """
        try:
            query_url = f"{self.wise_wfd_api}/0/query"
            
            params = {
                'where': where_clause,
                'outFields': '*',
                'returnGeometry': 'true',
                'geometryType': 'esriGeometryPoint', 
                'spatialRel': 'esriSpatialRelIntersects',
                'f': 'json',
                'resultRecordCount': limit
            }
            
            logger.info(f"Querying WISE WFD API: {where_clause}")
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' not in data:
                logger.warning("No 'features' in WFD API response")
                return []
            
            sites = []
            for feature in data['features']:
                attrs = feature.get('attributes', {})
                geom = feature.get('geometry', {})
                
                if 'x' in geom and 'y' in geom:
                    site = {
                        'site_id': attrs.get('monitoringSiteIdentifier') or attrs.get('SITE_ID') or attrs.get('ID'),
                        'site_name': attrs.get('siteName') or attrs.get('SITE_NAME') or attrs.get('NAME'),
                        'country_code': attrs.get('countryCode') or attrs.get('COUNTRY_CODE') or attrs.get('COUNTRY'),
                        'longitude': geom['x'],
                        'latitude': geom['y'],
                        'water_category': attrs.get('waterCategory') or attrs.get('WATER_CATEGORY'),
                        'data_source': 'eea_wise_wfd_2016',
                        'confidence': 1.0,
                        'layer': 'WFD_MonitoringSites_2016'
                    }
                    sites.append(site)
            
            logger.info(f"Retrieved {len(sites)} sites from WISE WFD API")
            return sites
            
        except Exception as e:
            logger.error(f"Error querying WISE WFD sites: {e}")
            return []
    
    def query_wfd_2022_sites(self, where_clause: str = "1=1", limit: int = 1000) -> List[Dict]:
        """
        Query WFD 2022 monitoring sites API.
        
        Args:
            where_clause: SQL WHERE clause for filtering
            limit: Maximum number of records to return
            
        Returns:
            List of monitoring site records with coordinates
        """
        try:
            query_url = f"{self.wfd_2022_api}/0/query"
            
            params = {
                'where': where_clause,
                'outFields': '*',
                'returnGeometry': 'true',
                'geometryType': 'esriGeometryPoint',
                'spatialRel': 'esriSpatialRelIntersects', 
                'f': 'json',
                'resultRecordCount': limit
            }
            
            logger.info(f"Querying WFD 2022 API: {where_clause}")
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' not in data:
                logger.warning("No 'features' in WFD 2022 API response")
                return []
            
            sites = []
            for feature in data['features']:
                attrs = feature.get('attributes', {})
                geom = feature.get('geometry', {})
                
                # Use lat/lon fields from attributes if available
                lat = attrs.get('lat') or geom.get('y')
                lon = attrs.get('lon') or geom.get('x')
                
                if lat is not None and lon is not None:
                    site = {
                        'site_id': (attrs.get('thematicIdIdentifier') or 
                                   attrs.get('inspireIdLocalId') or 
                                   attrs.get('monitoringSiteIdentifier')),
                        'site_name': (attrs.get('nameTextInternational') or 
                                     attrs.get('nameText') or 
                                     attrs.get('siteName') or 
                                     'UNKNOWN'),
                        'country_code': (attrs.get('countryCode') or 
                                       attrs.get('COUNTRY_CODE')),
                        'longitude': lon,
                        'latitude': lat,
                        'water_category': attrs.get('waterCategory'),
                        'data_source': 'eea_wfd_2022_monitoring_sites',
                        'confidence': 1.0,
                        'layer': 'WFD2022_MonitoringSite'
                    }
                    sites.append(site)
            
            logger.info(f"Retrieved {len(sites)} sites from WFD 2022 API")
            return sites
            
        except Exception as e:
            logger.error(f"Error querying WFD 2022 sites: {e}")
            return []
    
    def fetch_all_sites(self) -> List[Dict]:
        """Fetch all monitoring sites from available EEA APIs."""
        all_sites = []
        
        # Test API endpoints first
        api_status = self.test_api_endpoints()
        
        # Fetch from EIONET API
        if api_status.get('eionet', False):
            eionet_sites = self.query_eionet_sites()
            all_sites.extend(eionet_sites)
            time.sleep(1)  # Be respectful to the API
        
        # Fetch from WISE WFD API
        if api_status.get('wise_wfd', False):
            wfd_sites = self.query_wise_wfd_sites()
            all_sites.extend(wfd_sites)
            time.sleep(1)
        
        # Remove duplicates based on site_id
        unique_sites = {}
        for site in all_sites:
            site_id = site.get('site_id')
            if site_id and site_id not in unique_sites:
                unique_sites[site_id] = site
        
        logger.info(f"Total unique sites fetched: {len(unique_sites)}")
        return list(unique_sites.values())
    
    def update_coordinate_database(self, sites: List[Dict]) -> Tuple[int, int]:
        """
        Update the coordinate database with fetched sites.
        
        Args:
            sites: List of site dictionaries
            
        Returns:
            Tuple of (inserted, updated) counts
        """
        if not sites:
            return 0, 0
        
        inserted = 0
        updated = 0
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for site in sites:
                    site_id = site.get('site_id')
                    if not site_id:
                        continue
                    
                    # Check if site already exists
                    cursor.execute(
                        "SELECT COUNT(*) FROM coordinates WHERE gwb_code = ?", 
                        (site_id,)
                    )
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        # Update existing record
                        cursor.execute("""
                            UPDATE coordinates SET
                                latitude = ?, longitude = ?, name = ?, country_code = ?,
                                data_source = ?, confidence = ?, layer = ?, created_at = CURRENT_TIMESTAMP
                            WHERE gwb_code = ?
                        """, (
                            site.get('latitude'), site.get('longitude'), site.get('site_name'),
                            site.get('country_code'), site.get('data_source'), site.get('confidence'),
                            site.get('layer'), site_id
                        ))
                        updated += 1
                    else:
                        # Insert new record
                        cursor.execute("""
                            INSERT INTO coordinates 
                            (gwb_code, latitude, longitude, country_code, name, data_source, confidence, layer, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, (
                            site_id, site.get('latitude'), site.get('longitude'), site.get('country_code'),
                            site.get('site_name'), site.get('data_source'), site.get('confidence'),
                            site.get('layer')
                        ))
                        inserted += 1
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating coordinate database: {e}")
            return inserted, updated
        
        logger.info(f"Database updated: {inserted} inserted, {updated} updated")
        return inserted, updated
    
    def run_update(self) -> Dict[str, int]:
        """Run the complete coordinate update process."""
        logger.info("Starting EEA coordinate data update...")
        
        # Fetch all sites
        sites = self.fetch_all_sites()
        
        if not sites:
            logger.warning("No sites fetched from EEA APIs")
            return {'fetched': 0, 'inserted': 0, 'updated': 0}
        
        # Update database
        inserted, updated = self.update_coordinate_database(sites)
        
        result = {
            'fetched': len(sites),
            'inserted': inserted,
            'updated': updated
        }
        
        logger.info(f"Update complete: {result}")
        return result

def main():
    """Main function to run the coordinate fetcher."""
    fetcher = EEACoordinateFetcher()
    results = fetcher.run_update()
    
    print(f"""
EEA Coordinate Update Results:
==============================
Sites fetched: {results['fetched']}
Records inserted: {results['inserted']}
Records updated: {results['updated']}
""")

if __name__ == "__main__":
    main()