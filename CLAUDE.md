# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EEA Data API Service - A Python application to retrieve water quality disaggregated data from the European Environment Agency (EEA) WISE_SOE database via Dremio data lake.

## Development Commands

### Environment Setup
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Application

#### Web API Service
```bash
# Start the FastAPI web service
python app.py

# Or run directly with uvicorn
uvicorn src.api_server:app --host 127.0.0.1 --port 8000 --reload
```

#### Demo Scripts
```bash
# Run advanced examples (optional - shows direct API usage)
python example_usage.py
```

### Development
```bash
# Install packages in development mode
pip install -e .
```

## Architecture

### Core Components

- `src/dremio_service.py`: Main Dremio API service class (`DremioApiService`) for direct Dremio data lake access
- `src/unified_data_service.py`: Unified service that provides optimized Dremio data lake access
- `src/api_server.py`: FastAPI web service providing REST API endpoints
- `src/coordinate_service.py`: Service for enriching data with GPS coordinates
- `app.py`: Entry point to start the web service
- `example_usage.py`: Advanced usage examples with custom queries and data analysis
- `requirements.txt`: Project dependencies (requests, pandas, python-dotenv, fastapi, uvicorn, sqlite3)

### API Service Features

- Execute custom SQL queries against Dremio data lake
- Retrieve water quality disaggregated data from WISE_SOE database
- Convert results to pandas DataFrames for analysis
- Handle authentication and error management
- GPS coordinate enrichment for monitoring sites using proper EEA spatial identifiers
- Optimized coordinate enrichment using SQL JOIN queries
- Proper site identification using ThematicIdIdentifier + ThematicIdIdentifierScheme

### Web API Endpoints

- `GET /healthCheck`: Service status and health check
- `GET /waterbase`: Get waterbase disaggregated data with optional country filtering
- `GET /waterbase/country/{country_code}`: Latest measurements per parameter by country
- `GET /waterbase/site/{site_identifier}`: Latest measurements per parameter by monitoring site
- `GET /coordinates/country/{country_code}`: GPS coordinates for sites in a country

### Data Sources

The system uses the following Dremio tables:

**Main Data Table:**
```
"Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_DisaggregatedData"
```

**Spatial Coordinates Table:**
```
"Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
```

**Site Identification:**
Sites are properly identified using the combination of:
- `ThematicIdIdentifier`: The site identifier
- `ThematicIdIdentifierScheme`: The identifier scheme (euMonitoringSiteCode, eionetMonitoringSiteCode, etc.)

This ensures accurate coordinate matching and prevents identifier conflicts between different schemes.

### API Documentation

When running the service, visit:
- `http://127.0.0.1:8000/docs` - Interactive Swagger UI documentation
- `http://127.0.0.1:8000/redoc` - ReDoc documentation

### Key Methods

- `execute_query()`: Execute arbitrary SQL queries against Dremio
- `get_waterbase_data()`: Fetch waterbase disaggregated data (primary method)
- `get_waterbase_dataframe()`: Get waterbase data as pandas DataFrame
- `query_to_dataframe()`: Convert query results to pandas DataFrame
- `get_latest_measurements_by_country()`: Get latest measurements by country
- `get_latest_measurements_by_site()`: Get latest measurements by monitoring site