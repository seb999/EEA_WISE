# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EEA Data API Service - A Python application to retrieve groundwater pollutant data from the European Environment Agency (EEA) DiscoData SQL API.

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

- `src/discodata_service.py`: Main API service class (`EEAApiService`) that handles communication with the EEA DiscoData API
- `src/api_server.py`: FastAPI web service providing REST API endpoints
- `app.py`: Entry point to start the web service
- `example_usage.py`: Advanced usage examples with custom queries and data analysis
- `requirements.txt`: Project dependencies (requests, pandas, python-dotenv, fastapi, uvicorn)

### API Service Features

- Execute custom SQL queries against EEA databases
- Retrieve groundwater pollutant data from WISE_WFD database  
- Convert results to pandas DataFrames for analysis
- Handle pagination and error management
- Configurable request parameters (page size, email, schema)

### Web API Endpoints

- `GET /`: API information and available endpoints
- `GET /health`: Health check endpoint
- `GET /groundwater`: Get groundwater data with optional country filtering
- `GET /groundwater/countries`: List available countries
- `GET /groundwater/pollutants`: List pollutants (optionally by country)
- `GET /groundwater/summary`: Get summary statistics

### API Documentation

When running the service, visit:
- `http://127.0.0.1:8000/docs` - Interactive Swagger UI documentation
- `http://127.0.0.1:8000/redoc` - ReDoc documentation

### Key Methods

- `execute_query()`: Execute arbitrary SQL queries
- `get_groundwater_pollutant_data()`: Fetch specific groundwater data
- `query_to_dataframe()`: Convert query results to pandas DataFrame
- `get_groundwater_pollutant_dataframe()`: Get groundwater data as DataFrame