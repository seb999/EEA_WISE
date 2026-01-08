# OGC Compliance Implementation

## Overview

This document describes the OGC (Open Geospatial Consortium) compliance features added to the EEA WISE Data API service. The implementation focuses on providing GeoJSON output format for spatial data, which is the first step toward full OGC API - Features compliance.

## What Was Implemented

### 1. GeoJSON Formatter (`src/geojson_formatter.py`)

A new module that converts EEA WISE monitoring site data to OGC-compliant GeoJSON format:

**Key Features:**
- Converts individual records to GeoJSON Features with Point geometries
- Creates GeoJSON FeatureCollections from lists of data
- Handles coordinate extraction from various data formats
- Includes OGC-compliant metadata (numberMatched, numberReturned)
- Supports proper GeoJSON coordinate order: [longitude, latitude]

**Main Methods:**
- `to_feature()`: Convert single record to GeoJSON Feature
- `to_feature_collection()`: Convert list to FeatureCollection
- `format_spatial_locations()`: Format spatial object data
- `format_measurements_with_location()`: Format measurements with coordinates

### 2. New OGC-Compliant Endpoint

**`GET /ogc/spatial-locations`**

Primary OGC-compliant endpoint that returns monitoring site locations as GeoJSON FeatureCollection.

**Data Source:** `Waterbase_S_WISE_SpatialObject_DerivedData` table

**Parameters:**
- `country_code` (optional): Filter by country (e.g., 'FR', 'DE')
- `limit`: Maximum features to return (1-10000, default: 1000)
- `bbox` (optional): Bounding box filter - `minLon,minLat,maxLon,maxLat`

**Example Requests:**
```bash
# Get monitoring sites in France
GET /ogc/spatial-locations?country_code=FR&limit=100

# Get sites within a bounding box (Paris region)
GET /ogc/spatial-locations?bbox=1.0,48.0,3.0,49.5

# Get sites in Germany with limit
GET /ogc/spatial-locations?country_code=DE&limit=500
```

**Response Format:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "FRFR05026000",
      "geometry": {
        "type": "Point",
        "coordinates": [2.3522, 48.8566]
      },
      "properties": {
        "thematicIdIdentifier": "FRFR05026000",
        "thematicIdIdentifierScheme": "euMonitoringSiteCode",
        "monitoringSiteIdentifier": "FRFR05026000",
        "monitoringSiteName": "Seine at Paris",
        "countryCode": "FR"
      }
    }
  ],
  "numberMatched": 100,
  "numberReturned": 100,
  "timeStamp": "2026-01-08T12:00:00Z",
  "links": [
    {
      "href": "/ogc/spatial-locations",
      "rel": "self",
      "type": "application/geo+json",
      "title": "This document"
    }
  ]
}
```

### 3. Enhanced Existing Endpoints with GeoJSON Support

The following endpoints now support the `format=geojson` parameter for OGC-compliant output:

**`GET /waterbase`**
- Get disaggregated water quality data
- Add `?format=geojson` to get measurements as GeoJSON Features

**`GET /waterbase/country/{country_code}`**
- Get latest measurements per parameter by country
- Add `?format=geojson` to get measurements as GeoJSON Features

**`GET /waterbase/site/{site_identifier}`**
- Get latest measurements per parameter by monitoring site
- Add `?format=geojson` to get measurements as GeoJSON Features

**Examples:**
```bash
# Get water quality data for France as GeoJSON
GET /waterbase?country_code=FR&limit=100&format=geojson

# Get latest measurements for France as GeoJSON
GET /waterbase/country/FR?format=geojson

# Get latest measurements for a specific site as GeoJSON
GET /waterbase/site/FRFR05026000?format=geojson
```

### 4. Removed Legacy Endpoints

The following endpoints have been **removed** in favor of the OGC-compliant `/ogc/spatial-locations`:

- ❌ `GET /sites` - Replaced by `/ogc/spatial-locations`
- ❌ `GET /coordinates/country/{country_code}` - Replaced by `/ogc/spatial-locations?country_code={code}`

**Migration:**
```bash
# Old (removed)
GET /coordinates/country/FR?limit=100&format=geojson

# New (use this)
GET /ogc/spatial-locations?country_code=FR&limit=100
```

## OGC Compliance Features

### ✅ Implemented

1. **GeoJSON Output Format**
   - Valid GeoJSON FeatureCollection structure
   - Proper Point geometry with [lon, lat] coordinate order
   - Feature IDs using site identifiers
   - Properties contain all non-spatial attributes

2. **OGC Metadata Fields**
   - `numberMatched`: Total records matching query
   - `numberReturned`: Features in response
   - `timeStamp`: ISO 8601 timestamp
   - `links`: Related resource links

3. **Spatial Filtering**
   - Bounding box (`bbox`) parameter support
   - Country-based filtering
   - Coordinate validation

4. **Proper CRS (Coordinate Reference System)**
   - Uses WGS 84 (EPSG:4326) - the GeoJSON default
   - Coordinates in decimal degrees

### 🔄 Partially Implemented

1. **Content Negotiation**
   - Uses query parameter (`?format=geojson`) instead of Accept header
   - Could be enhanced to support `Accept: application/geo+json`

### ❌ Not Yet Implemented

Future enhancements for full OGC API - Features compliance:

1. **OGC API - Features Core Endpoints**
   - `GET /conformance`: Declare conformance classes
   - `GET /collections`: List available collections
   - `GET /collections/{collectionId}`: Collection metadata
   - `GET /collections/{collectionId}/items`: Features in collection

2. **Advanced Query Capabilities**
   - CQL2 filtering (Common Query Language)
   - Datetime filtering with ISO 8601 intervals
   - Property filters

3. **Pagination**
   - `offset` parameter
   - `next` and `prev` links

4. **OGC SensorThings API**
   - Thing, Sensor, Datastream entities
   - Observations with time-series data
   - MQTT support for real-time data

## Testing

A comprehensive test suite has been created: **`test_geojson.py`**

**Run Tests:**
```bash
# 1. Start the API server
python app.py

# 2. In another terminal, run tests
python test_geojson.py
```

**Test Coverage:**
- OGC spatial locations endpoint validation
- GeoJSON structure verification
- Bounding box filtering
- Waterbase data in GeoJSON format
- Coordinates endpoint with GeoJSON
- Sample GeoJSON generation

## Usage Examples

### Example 1: View Monitoring Sites in QGIS

```bash
# Get GeoJSON data
curl "http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=1000" \
  -o france_sites.geojson

# Open france_sites.geojson in QGIS
# Layer → Add Layer → Add Vector Layer
```

### Example 2: Web Mapping with Leaflet

```javascript
fetch('http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=100')
  .then(response => response.json())
  .then(geojson => {
    L.geoJSON(geojson, {
      onEachFeature: function(feature, layer) {
        layer.bindPopup(`
          <b>${feature.properties.monitoringSiteName}</b><br>
          Site ID: ${feature.id}
        `);
      }
    }).addTo(map);
  });
```

### Example 3: Filter by Bounding Box

```bash
# Get sites in Paris region
curl "http://127.0.0.1:8000/ogc/spatial-locations?bbox=2.2,48.8,2.5,48.9"
```

### Example 4: Water Quality Measurements as GeoJSON

```bash
# Get measurements with locations
curl "http://127.0.0.1:8000/waterbase?country_code=FR&limit=50&format=geojson" \
  -o measurements.geojson
```

## Benefits of OGC Compliance

1. **Interoperability**: Data can be consumed by any OGC-compliant GIS tool
2. **Standardization**: Follows international geospatial standards
3. **Tool Support**: Works with QGIS, ArcGIS, Leaflet, OpenLayers, etc.
4. **Web Mapping**: Direct integration with web mapping libraries
5. **Data Sharing**: Easier data exchange between organizations
6. **Future-Proof**: Built on stable, widely-adopted standards

## Next Steps for Full OGC Compliance

### Phase 2: OGC API - Features Core

1. Implement `/conformance` endpoint
2. Implement `/collections` structure
3. Add CQL2 filtering support
4. Add pagination with links

### Phase 3: OGC SensorThings API

1. Map water quality data to SensorThings entities
2. Implement Thing/Location/Datastream/Observation endpoints
3. Add temporal filtering with ISO 8601
4. Consider MQTT support for real-time data

### Phase 4: Additional Standards

1. **OGC API - Tiles**: Vector tiles for large datasets
2. **OGC API - Records**: Metadata catalog
3. **STAC (SpatioTemporal Asset Catalog)**: Discovery metadata

## Technical Details

### Coordinate Reference System

- **CRS**: WGS 84 (EPSG:4326)
- **Coordinate Order**: Longitude, Latitude (per GeoJSON spec)
- **Format**: Decimal degrees

### Data Sources

The OGC endpoints use the spatial object table:
```sql
"Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
```

**Key Fields:**
- `thematicIdIdentifier`: Site identifier
- `thematicIdIdentifierScheme`: Identifier scheme
- `lat`, `lon`: Coordinates in WGS 84
- `monitoringSiteName`: Human-readable site name
- `countryCode`: ISO country code

## Dependencies

New dependency added:
```
geojson>=3.0.0
```

Install with:
```bash
pip install -r requirements.txt
```

## API Documentation

When the service is running, visit:
- **Swagger UI**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc

All new endpoints and parameters are documented in the interactive API documentation.

## Backward Compatibility

All existing endpoints remain unchanged by default:
- Original JSON format is the default response
- GeoJSON is opt-in via `?format=geojson` parameter
- No breaking changes to existing API consumers

## Contributing

To extend OGC compliance:

1. Review OGC standards at https://www.ogc.org/standards/
2. Use `GeoJSONFormatter` class for consistent output
3. Add new endpoints under `/ogc/` prefix
4. Update tests in `test_geojson.py`
5. Document in this file

## References

- **OGC API - Features**: https://ogcapi.ogc.org/features/
- **GeoJSON Specification**: https://datatracker.ietf.org/doc/html/rfc7946
- **OGC SensorThings API**: https://www.ogc.org/standards/sensorthings
- **EEA Waterbase**: https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-icm
