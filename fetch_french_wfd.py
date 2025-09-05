"""
Fetch comprehensive French monitoring sites from WFD 2022 API
"""

from src.eea_coordinate_fetcher import EEACoordinateFetcher
import requests
import time

print('Fetching comprehensive French WFD 2022 monitoring sites...')
print('This may take a few minutes due to the large dataset (7,355 sites)')

fetcher = EEACoordinateFetcher()

# Fetch French sites with pagination
all_french_sites = []
batch_size = 2000
offset = 0
max_sites = 8000  # Safety limit

while len(all_french_sites) < max_sites:
    print(f'Fetching batch starting at offset {offset}...')
    
    try:
        query_url = f'{fetcher.wfd_2022_api}/0/query'
        params = {
            'where': "countryCode = 'FR'",
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
                print(f'No more features found. Stopping at offset {offset}')
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
                        'data_source': 'eea_wfd_2022_monitoring_sites',
                        'confidence': 1.0,
                        'layer': 'WFD2022_MonitoringSite'
                    }
                    batch_sites.append(site)
            
            all_french_sites.extend(batch_sites)
            print(f'  Retrieved {len(batch_sites)} sites from this batch')
            print(f'  Total so far: {len(all_french_sites)} sites')
            
            # Check if we got fewer results than requested
            if len(features) < batch_size:
                print('Reached end of data')
                break
                
            offset += batch_size
            time.sleep(1)  # Be respectful to the API
            
        else:
            print(f'Request failed with status {response.status_code}')
            break
            
    except Exception as e:
        print(f'Error in batch fetch: {e}')
        break

print(f'Successfully fetched {len(all_french_sites)} French WFD 2022 monitoring sites!')

# Update the database
if all_french_sites:
    print('Updating coordinate database...')
    inserted, updated = fetcher.update_coordinate_database(all_french_sites)
    print(f'Database update complete: {inserted} inserted, {updated} updated')
    
    # Show some examples
    print('Sample new French sites:')
    for i, site in enumerate(all_french_sites[:3]):
        print(f'  {i+1}. {site["site_id"]}: {site["site_name"]} ({site["latitude"]}, {site["longitude"]})')
else:
    print('No French sites to update')