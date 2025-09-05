import sqlite3
import os
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class CoordinateService:
    """Service to retrieve geographic coordinates for monitoring sites from SQLite database."""
    
    def __init__(self, db_path: str = None):
        """
        Initialize the coordinate service.
        
        Args:
            db_path: Path to the SQLite coordinates database
        """
        if db_path is None:
            # Default path relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(current_dir, 'coordinates.db')
        
        self.db_path = db_path
        self._verify_database()
    
    def _verify_database(self) -> bool:
        """Verify that the database exists and has the expected structure."""
        try:
            if not os.path.exists(self.db_path):
                logger.warning(f"Coordinates database not found at {self.db_path}")
                return False
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='coordinates'")
                if not cursor.fetchone():
                    logger.warning("Coordinates table not found in database")
                    return False
                    
            logger.info(f"✓ Coordinate database verified at {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying coordinate database: {e}")
            return False
    
    def get_coordinates_for_site(self, site_id: str, country_code: str = None) -> Optional[Dict[str, Any]]:
        """
        Get coordinates for a specific monitoring site using multiple matching strategies.
        
        Args:
            site_id: Monitoring site identifier
            country_code: Optional country code for better matching
            
        Returns:
            Dictionary with latitude, longitude and other coordinate info, or None if not found
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Strategy 1: Try exact match first
                if country_code:
                    cursor.execute(
                        "SELECT *, 1.0 as match_confidence FROM coordinates WHERE gwb_code = ? AND country_code = ? LIMIT 1",
                        (site_id, country_code)
                    )
                else:
                    cursor.execute(
                        "SELECT *, 1.0 as match_confidence FROM coordinates WHERE gwb_code = ? LIMIT 1",
                        (site_id,)
                    )
                
                result = cursor.fetchone()
                if result:
                    return self._format_coordinate_result(result, result['match_confidence'])
                
                # Strategy 2: Try matching by country prefix (e.g., ES020... -> ES015)
                if country_code and len(site_id) > 2:
                    # Extract country prefix and numbers from site_id (e.g., ES020ESBT... -> ES020, ES0, etc.)
                    prefixes_to_try = [
                        site_id[:5],  # ES020
                        site_id[:4],  # ES02
                        site_id[:3],  # ES0
                        site_id[:2]   # ES
                    ]
                    
                    for prefix in prefixes_to_try:
                        cursor.execute(
                            "SELECT *, 0.8 as match_confidence FROM coordinates WHERE gwb_code LIKE ? AND country_code = ? LIMIT 1",
                            (f"{prefix}%", country_code)
                        )
                        result = cursor.fetchone()
                        if result:
                            logger.info(f"Found coordinate match using prefix '{prefix}' for site {site_id}")
                            return self._format_coordinate_result(result, result['match_confidence'])
                
                # Strategy 3: Try partial matching within country
                if country_code:
                    cursor.execute(
                        "SELECT *, 0.6 as match_confidence FROM coordinates WHERE country_code = ? AND (gwb_code LIKE ? OR gwb_code LIKE ?) LIMIT 1",
                        (country_code, f"%{site_id[:6]}%", f"{country_code}%")
                    )
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Found coordinate match using partial matching for site {site_id}")
                        return self._format_coordinate_result(result, result['match_confidence'])
                
                # Strategy 4: Fallback - get any coordinate from the same country (lowest confidence)
                if country_code:
                    cursor.execute(
                        "SELECT *, 0.3 as match_confidence FROM coordinates WHERE country_code = ? LIMIT 1",
                        (country_code,)
                    )
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Found fallback coordinate for country {country_code} for site {site_id}")
                        return self._format_coordinate_result(result, result['match_confidence'])
                
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving coordinates for site {site_id}: {e}")
            return None
    
    def _format_coordinate_result(self, result, match_confidence: float) -> Dict[str, Any]:
        """Format coordinate result with simplified structure."""
        return {
            'latitude': result['latitude'],
            'longitude': result['longitude'],
            'name': result['name'],
            'match_with': result['gwb_code']
        }
    
    def get_coordinates_by_country(self, country_code: str, limit: int = 1000) -> list:
        """
        Get all coordinates for a specific country.
        
        Args:
            country_code: Country code (e.g., 'DE', 'FR')
            limit: Maximum number of results to return
            
        Returns:
            List of coordinate dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute(
                    "SELECT * FROM coordinates WHERE country_code = ? LIMIT ?",
                    (country_code, limit)
                )
                
                results = cursor.fetchall()
                
                return [
                    {
                        'gwb_code': row['gwb_code'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'country_code': row['country_code'],
                        'name': row['name'],
                        'data_source': row['data_source'],
                        'confidence': row['confidence'],
                        'layer': row['layer']
                    }
                    for row in results
                ]
                
        except Exception as e:
            logger.error(f"Error retrieving coordinates for country {country_code}: {e}")
            return []
    
    def search_coordinates(self, search_term: str, limit: int = 100) -> list:
        """
        Search coordinates by site name or code.
        
        Args:
            search_term: Search term to match against site names or codes
            limit: Maximum number of results to return
            
        Returns:
            List of coordinate dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                search_pattern = f"%{search_term}%"
                cursor.execute(
                    """SELECT * FROM coordinates 
                       WHERE gwb_code LIKE ? OR name LIKE ? 
                       LIMIT ?""",
                    (search_pattern, search_pattern, limit)
                )
                
                results = cursor.fetchall()
                
                return [
                    {
                        'gwb_code': row['gwb_code'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'country_code': row['country_code'],
                        'name': row['name'],
                        'data_source': row['data_source'],
                        'confidence': row['confidence'],
                        'layer': row['layer']
                    }
                    for row in results
                ]
                
        except Exception as e:
            logger.error(f"Error searching coordinates with term '{search_term}': {e}")
            return []
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get statistics about the coordinate database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total count
                cursor.execute("SELECT COUNT(*) FROM coordinates")
                total_count = cursor.fetchone()[0]
                
                # Count by country
                cursor.execute("SELECT country_code, COUNT(*) FROM coordinates GROUP BY country_code ORDER BY COUNT(*) DESC")
                country_counts = dict(cursor.fetchall())
                
                # Data source breakdown
                cursor.execute("SELECT data_source, COUNT(*) FROM coordinates GROUP BY data_source")
                source_counts = dict(cursor.fetchall())
                
                return {
                    'total_coordinates': total_count,
                    'countries': len(country_counts),
                    'country_breakdown': country_counts,
                    'data_sources': source_counts
                }
                
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {'error': str(e)}