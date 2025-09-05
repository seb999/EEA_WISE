"""
Fetch comprehensive monitoring sites for top EU countries from WFD 2022 API
This will dramatically improve coordinate coverage across Europe
"""

from src.eea_coordinate_fetcher import EEACoordinateFetcher
import requests
import time

# Top 10 EU countries by WFD 2022 monitoring site coverage
TOP_EU_COUNTRIES = [
    ('DK', 35202),  # Denmark
    ('DE', 34999),  # Germany  
    ('FI', 14923),  # Finland
    ('IT', 14315),  # Italy
    ('ES', 14148),  # Spain
    ('SE', 12370),  # Sweden
    ('AT', 9617),   # Austria
    ('FR', 7355),   # France (already done, but included for completeness)
    ('IE', 7338),   # Ireland
    ('PL', 5011),   # Poland
]

def fetch_country_wfd_sites(fetcher, country_code, expected_count):
    """Fetch WFD 2022 monitoring sites for a specific country."""
    print(f'Fetching {country_code} monitoring sites (expected: {expected_count:,})...')
    
    all_sites = []
    batch_size = 2000
    offset = 0
    
    while len(all_sites) < expected_count + 100:  # Add buffer for safety
        try:
            query_url = f'{fetcher.wfd_2022_api}/0/query'
            params = {
                'where': f"countryCode = '{country_code}'",
                'outFields': '*',
                'returnGeometry': 'true',
                'f': 'json',
                'resultRecordCount': batch_size,
                'resultOffset': offset
            }
            
            response = requests.get(query_url, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                features = data.get('features', [])
                
                if not features:
                    print(f'  No more features for {country_code} at offset {offset}')
                    break
                
                # Process this batch
                batch_sites = []
                for feature in features:
                    attrs = feature.get('attributes', {})
                    geom = feature.get('geometry', {})
                    
                    lat = attrs.get('lat') or geom.get('y')
                    lon = attrs.get('lon') or geom.get('x')
                    
                    if lat is not None and lon is not None:
                        site = {
                            'site_id': (attrs.get('thematicIdIdentifier') or 
                                       attrs.get('inspireIdLocalId')),
                            'site_name': (attrs.get('nameTextInternational') or 
                                         attrs.get('nameText') or 'UNKNOWN'),
                            'country_code': attrs.get('countryCode'),
                            'longitude': lon,
                            'latitude': lat,
                            'water_category': attrs.get('waterCategory'),
                            'data_source': f'eea_wfd_2022_{country_code.lower()}',
                            'confidence': 1.0,
                            'layer': 'WFD2022_MonitoringSite'
                        }
                        batch_sites.append(site)
                
                all_sites.extend(batch_sites)
                
                if len(all_sites) % 5000 == 0:  # Progress update every 5k sites
                    print(f'  {country_code}: {len(all_sites):,} sites fetched so far...')
                
                # Check if we got fewer results than requested
                if len(features) < batch_size:
                    break
                    
                offset += batch_size
                time.sleep(0.5)  # Be respectful to API
                
            else:
                print(f'  Request failed for {country_code}: {response.status_code}')
                break
                
        except Exception as e:
            print(f'  Error fetching {country_code} batch: {e}')
            break
    
    print(f'  {country_code}: Successfully fetched {len(all_sites):,} sites')
    return all_sites

def main():
    """Main function to fetch comprehensive EU monitoring sites."""
    print('Starting comprehensive EU monitoring sites fetch...')
    print('This will add 100,000+ monitoring sites to the database!')
    print()
    
    fetcher = EEACoordinateFetcher()
    
    # Statistics tracking
    total_sites_fetched = 0
    total_inserted = 0
    total_updated = 0
    
    # Process each country
    for country_code, expected_count in TOP_EU_COUNTRIES:
        if country_code == 'FR':
            print(f'Skipping {country_code} - already processed (7,355 sites)')
            continue
            
        try:
            # Fetch sites for this country
            country_sites = fetch_country_wfd_sites(fetcher, country_code, expected_count)
            
            if country_sites:
                total_sites_fetched += len(country_sites)
                
                # Update database
                print(f'  Updating database with {len(country_sites):,} {country_code} sites...')
                inserted, updated = fetcher.update_coordinate_database(country_sites)
                total_inserted += inserted
                total_updated += updated
                
                print(f'  {country_code} complete: {inserted:,} inserted, {updated:,} updated')
                print()
            else:
                print(f'  No sites fetched for {country_code}')
                print()
                
        except Exception as e:
            print(f'Error processing {country_code}: {e}')
            print()
    
    # Final summary
    print('=' * 60)
    print('COMPREHENSIVE EU MONITORING SITES FETCH COMPLETE!')
    print('=' * 60)
    print(f'Countries processed: {len(TOP_EU_COUNTRIES) - 1}')  # Minus France
    print(f'Total sites fetched: {total_sites_fetched:,}')
    print(f'Database records inserted: {total_inserted:,}')
    print(f'Database records updated: {total_updated:,}')
    print()
    print('Your coordinate database now has comprehensive coverage across major EU countries!')

if __name__ == "__main__":
    main()