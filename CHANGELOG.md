# Changelog

## [3.2.0] - 2026-01-08

### Added
- **OGC-compliant GeoJSON output** for spatial data
- New endpoint `GET /ogc/spatial-locations` - Primary OGC-compliant endpoint for monitoring site locations
  - Returns GeoJSON FeatureCollection format
  - Supports `country_code` filter
  - Supports `bbox` (bounding box) spatial filtering
  - Includes OGC metadata (numberMatched, numberReturned, timeStamp, links)
- GeoJSON formatter module (`src/geojson_formatter.py`)
  - Converts spatial data to RFC 7946 compliant GeoJSON
  - Handles Point geometries with proper [lon, lat] coordinate order
  - Supports feature properties and metadata
- Enhanced `GET /waterbase` endpoint with `format=geojson` parameter
  - Water quality measurements can now be returned as GeoJSON Features
- Comprehensive test suite (`test_geojson.py`)
  - Tests OGC compliance
  - Validates GeoJSON structure
  - Tests bounding box filtering
  - Generates sample GeoJSON files
- Documentation (`OGC_COMPLIANCE.md`)
  - Complete OGC implementation guide
  - Usage examples
  - Migration guide
  - Future roadmap

### Changed
- API version updated to 3.2.0
- Enhanced documentation in `CLAUDE.md`

### Removed
- `GET /sites` endpoint - Replaced by `/ogc/spatial-locations`
- `GET /coordinates/country/{country_code}` endpoint - Replaced by `/ogc/spatial-locations`

### Migration Guide

**For spatial location queries:**
```bash
# Old (removed)
GET /coordinates/country/FR?limit=100

# New (use this)
GET /ogc/spatial-locations?country_code=FR&limit=100
```

```bash
# Old (removed)
GET /sites?country_code=DE

# New (use this)
GET /ogc/spatial-locations?country_code=DE
```

**All queries now return GeoJSON by default** for the `/ogc/spatial-locations` endpoint.

### Technical Details

**Dependencies Added:**
- `geojson>=3.0.0`

**New Files:**
- `src/geojson_formatter.py` - GeoJSON formatting utilities
- `test_geojson.py` - OGC compliance test suite
- `OGC_COMPLIANCE.md` - OGC implementation documentation
- `CHANGELOG.md` - This file

**Modified Files:**
- `src/api_server.py` - Added OGC endpoint, removed legacy endpoints
- `CLAUDE.md` - Updated endpoint documentation
- `requirements.txt` - Added geojson dependency

### Benefits

1. **Standards Compliance**: API now follows OGC GeoJSON standards (RFC 7946)
2. **GIS Tool Compatibility**: Works with QGIS, ArcGIS, Leaflet, OpenLayers
3. **Spatial Filtering**: Bounding box support for spatial queries
4. **Cleaner API**: Removed redundant endpoints
5. **Better Documentation**: Comprehensive OGC compliance guide

### Breaking Changes

⚠️ **The following endpoints have been removed:**
- `GET /sites`
- `GET /coordinates/country/{country_code}`

**Action Required:** Update API consumers to use `/ogc/spatial-locations` instead.

### Testing

Run the test suite:
```bash
# Start the API server
python app.py

# Run tests (in another terminal)
python test_geojson.py
```

### Next Steps

This release implements **Phase 1** of OGC compliance (GeoJSON output).

**Phase 2** (Future):
- OGC API - Features Core endpoints (`/collections`, `/conformance`)
- CQL2 filtering support
- Pagination with links

**Phase 3** (Future):
- OGC SensorThings API for time-series data
- Real-time data streaming
- MQTT support

---

## [3.1.0] - Previous Release
- Time-series data retrieval with aggregation
- Metadata discovery endpoints
- GPS coordinate enrichment
- Optimized coordinate JOIN queries
