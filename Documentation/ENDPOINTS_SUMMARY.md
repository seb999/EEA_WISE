# API Endpoints Summary

## Current API Endpoints (v3.2.0)

### Health & Status
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthCheck` | GET | Service status and health check |

### Core Data Endpoints
| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/waterbase` | GET | `country_code`, `limit`, `format` | Get waterbase disaggregated data (supports GeoJSON) |
| `/waterbase/country/{country_code}` | GET | - | Latest measurements per parameter by country |
| `/waterbase/site/{site_identifier}` | GET | - | Latest measurements per parameter by monitoring site |

### Time-Series Endpoints
| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/timeseries/site/{site_identifier}` | GET | `parameter`, `start_date`, `end_date`, `interval` | Get time-series data with aggregation options |

### Metadata Endpoints
| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/parameters` | GET | - | List all available chemical parameters with measurement counts |

### OGC-Compliant Spatial Endpoints ⭐ NEW
| Endpoint | Method | Parameters | Description |
|----------|--------|------------|-------------|
| `/ogc/spatial-locations` | GET | `country_code`, `limit`, `bbox` | Get monitoring site locations as GeoJSON FeatureCollection |

---

## Endpoint Details

### `/ogc/spatial-locations` ⭐ Primary OGC Endpoint

**Returns:** GeoJSON FeatureCollection

**Parameters:**
- `country_code` (optional): ISO country code (e.g., 'FR', 'DE', 'ES')
- `limit` (optional): Maximum features to return (1-10000, default: 1000)
- `bbox` (optional): Bounding box filter - `minLon,minLat,maxLon,maxLat`

**Examples:**
```bash
# Get all sites in France
GET /ogc/spatial-locations?country_code=FR&limit=100

# Get sites in Paris region (bounding box)
GET /ogc/spatial-locations?bbox=2.2,48.8,2.5,48.9

# Get sites in Germany with custom limit
GET /ogc/spatial-locations?country_code=DE&limit=500
```

**Response Format:** OGC-compliant GeoJSON FeatureCollection
```json
{
  "type": "FeatureCollection",
  "features": [...],
  "numberMatched": 100,
  "numberReturned": 100,
  "timeStamp": "2026-01-08T12:00:00Z",
  "links": [...]
}
```

---

### `/waterbase`

**Returns:** JSON or GeoJSON (based on `format` parameter)

**Parameters:**
- `country_code` (optional): ISO country code filter
- `limit` (optional): Maximum records (1-300000, default: 1000)
- `format` (optional): Output format - `json` (default) or `geojson`

**Examples:**
```bash
# Get water quality data as JSON (default)
GET /waterbase?country_code=FR&limit=100

# Get water quality data as GeoJSON
GET /waterbase?country_code=FR&limit=100&format=geojson
```

---

### `/waterbase/country/{country_code}`

**Returns:** JSON or GeoJSON (based on `format` parameter) with latest measurements per parameter

**Path Parameters:**
- `country_code`: ISO country code (e.g., 'FR', 'DE')

**Query Parameters:**
- `format` (optional): Output format - `json` (default) or `geojson`

**Examples:**
```bash
# Get latest measurements as JSON (default)
GET /waterbase/country/FR

# Get latest measurements as GeoJSON
GET /waterbase/country/FR?format=geojson
```

---

### `/waterbase/site/{site_identifier}`

**Returns:** JSON or GeoJSON (based on `format` parameter) with latest measurements per parameter for a specific site

**Path Parameters:**
- `site_identifier`: Monitoring site identifier (e.g., 'FRFR05026000')

**Query Parameters:**
- `format` (optional): Output format - `json` (default) or `geojson`

**Examples:**
```bash
# Get latest measurements as JSON (default)
GET /waterbase/site/FRFR05026000

# Get latest measurements as GeoJSON
GET /waterbase/site/FRFR05026000?format=geojson
```

---

### `/timeseries/site/{site_identifier}`

**Returns:** JSON with time-series data

**Path Parameters:**
- `site_identifier`: Monitoring site identifier

**Query Parameters:**
- `parameter` (optional): Chemical parameter code (e.g., 'NO3', 'PO4')
- `start_date` (optional): Start date in YYYY-MM-DD format
- `end_date` (optional): End date in YYYY-MM-DD format
- `interval` (optional): Aggregation interval - `raw`, `monthly`, `yearly` (default: raw)

**Examples:**
```bash
# Get raw time-series for all parameters
GET /timeseries/site/FRFR05026000

# Get monthly aggregated data for nitrate
GET /timeseries/site/FRFR05026000?parameter=NO3&interval=monthly

# Get data for specific date range
GET /timeseries/site/FRFR05026000?start_date=2020-01-01&end_date=2023-12-31
```

---

### `/parameters`

**Returns:** JSON with available chemical parameters

**Example:**
```bash
GET /parameters
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "observedPropertyDeterminandCode": "NO3",
      "observedPropertyDeterminandLabel": "Nitrate",
      "resultUom": "mg/L",
      "measurement_count": 15432
    },
    ...
  ]
}
```

---

### `/healthCheck`

**Returns:** Service status and capabilities

**Example:**
```bash
GET /healthCheck
```

**Response:**
```json
{
  "service_status": {
    "data_service_available": true,
    "coordinates_available": true,
    "active_data_service": "dremio",
    "configured_mode": "dremio"
  },
  "api_version": "3.2.0",
  "features": {
    "data_connection": true,
    "coordinate_service": true,
    "ogc_geojson_support": true,
    "bbox_filtering": true,
    "switchable_backends": true
  }
}
```

---

## Removed Endpoints ❌

The following endpoints have been **removed** in v3.2.0:

| Removed Endpoint | Replacement | Migration |
|------------------|-------------|-----------|
| `GET /sites` | `GET /ogc/spatial-locations` | Use `/ogc/spatial-locations?country_code={code}` |
| `GET /coordinates/country/{code}` | `GET /ogc/spatial-locations` | Use `/ogc/spatial-locations?country_code={code}` |

---

## GeoJSON Support

The following endpoints support GeoJSON output:

1. **`/ogc/spatial-locations`** - Returns GeoJSON by default
2. **`/waterbase`** - Add `?format=geojson` parameter

**GeoJSON Features:**
- RFC 7946 compliant
- Point geometries with [longitude, latitude] order
- Proper feature properties
- OGC metadata (numberMatched, numberReturned, timeStamp)

---

## API Documentation

Interactive API documentation available at:
- **Swagger UI**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc

---

## Quick Start

```bash
# 1. Start the API server
python app.py

# 2. Test health check
curl http://127.0.0.1:8000/healthCheck

# 3. Get spatial locations for France (GeoJSON)
curl http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=10

# 4. Get water quality measurements
curl http://127.0.0.1:8000/waterbase?country_code=FR&limit=10

# 5. Get time-series data
curl http://127.0.0.1:8000/timeseries/site/FRFR05026000?interval=monthly
```

---

## Common Use Cases

### Use Case 1: Map Monitoring Sites in QGIS
```bash
# Export GeoJSON for a country
curl "http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=1000" \
  -o france_monitoring_sites.geojson

# Open in QGIS: Layer → Add Vector Layer → france_monitoring_sites.geojson
```

### Use Case 2: Get Latest Water Quality by Country
```bash
# Get latest measurements for each parameter in France
curl http://127.0.0.1:8000/waterbase/country/FR
```

### Use Case 3: Analyze Time-Series Trends
```bash
# Get monthly nitrate levels for a site over 5 years
curl "http://127.0.0.1:8000/timeseries/site/FRFR05026000?parameter=NO3&start_date=2018-01-01&end_date=2023-12-31&interval=monthly"
```

### Use Case 4: Web Map with Leaflet
```javascript
// Fetch GeoJSON and display on map
fetch('http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR')
  .then(res => res.json())
  .then(geojson => {
    L.geoJSON(geojson).addTo(map);
  });
```

### Use Case 5: Spatial Query with Bounding Box
```bash
# Get all sites within a specific geographic area
curl "http://127.0.0.1:8000/ogc/spatial-locations?bbox=2.2,48.8,2.5,48.9"
```

---

## Response Formats

### Standard JSON Response
```json
{
  "success": true,
  "data": [...],
  "metadata": {...}
}
```

### GeoJSON Response (OGC-compliant)
```json
{
  "type": "FeatureCollection",
  "features": [...],
  "numberMatched": 100,
  "numberReturned": 100,
  "timeStamp": "2026-01-08T12:00:00Z"
}
```

---

## Rate Limits & Constraints

| Parameter | Limit | Note |
|-----------|-------|------|
| `limit` for `/waterbase` | 1 - 300,000 | Default: 1000 |
| `limit` for `/ogc/spatial-locations` | 1 - 10,000 | Default: 1000 |
| Bounding box format | `minLon,minLat,maxLon,maxLat` | WGS84 decimal degrees |

---

## Support & Documentation

- **Full OGC Documentation**: See `OGC_COMPLIANCE.md`
- **Changelog**: See `CHANGELOG.md`
- **Project Guide**: See `CLAUDE.md`
- **Interactive API Docs**: http://127.0.0.1:8000/docs
