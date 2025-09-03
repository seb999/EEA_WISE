from fastapi import FastAPI, HTTPException, Query
from typing import Optional, Dict, Any
import uvicorn
import pandas as pd
from .eea_api_service import EEAApiService

app = FastAPI(
    title="EEA Water Data API",
    description="API service to retrieve water quality data from the European Environment Agency (EEA) WISE_SOE database",
    version="1.0.0"
)

# Initialize the EEA API service
eea_service = EEAApiService()


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "EEA Water Data API",
        "version": "1.0.0",
        "endpoints": {
            "/waterbase": "Get waterbase aggregated data",
            "/waterbase/countries": "Get available countries",
            "/waterbase/parameters": "Get water parameters list",
            "/waterbase/summary": "Get waterbase data summary",
            "/health": "Health check"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "EEA Water Data API"}


@app.get("/waterbase")
async def get_waterbase_data(
    country: Optional[str] = Query(None, description="Filter by country code (e.g., 'DE', 'DK', 'FI')"),
    parameter: Optional[str] = Query(None, description="Filter by parameter code"),
    year: Optional[int] = Query(None, description="Filter by reference year"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    page: int = Query(1, ge=1, description="Page number"),
    hits_per_page: int = Query(50, ge=1, le=100, description="Records per page")
) -> Dict[str, Any]:
    """
    Get waterbase aggregated data with optional filtering.
    
    Args:
        country: Optional country code filter (e.g., 'DE' for Germany)
        parameter: Optional parameter code filter
        year: Optional reference year filter
        limit: Maximum number of records to return (1-1000)
        page: Page number (starting from 1)
        hits_per_page: Number of records per page (1-100)
        
    Returns:
        JSON response with waterbase data
    """
    try:
        # Build query with optional filters
        conditions = []
        if country:
            country = country.upper()  # Normalize to uppercase
            conditions.append(f"countryCode = '{country}'")
        if parameter:
            conditions.append(f"parameterWaterCategoryCode = '{parameter}'")
        if year:
            conditions.append(f"phenomenonTimeReferenceYear = {year}")
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        query = f"""
        SELECT TOP {limit} * 
        FROM [WISE_SOE].[latest].[Waterbase_T_WISE6_AggregatedData] 
        {where_clause}
        ORDER BY phenomenonTimeReferenceYear DESC
        """
        
        # Execute query
        response = eea_service.execute_query(query, page, hits_per_page)
        
        # Format response
        result = {
            "success": True,
            "filters": {
                "country": country,
                "parameter": parameter,
                "year": year,
                "limit": limit,
                "page": page,
                "hits_per_page": hits_per_page
            },
            "data": response.get("results", []),
            "metadata": {
                "total_records": len(response.get("results", [])),
                "page": page,
                "has_more_pages": len(response.get("results", [])) == hits_per_page
            }
        }
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch waterbase data: {str(e)}"
        )


@app.get("/waterbase/countries")
async def get_available_countries() -> Dict[str, Any]:
    """
    Get list of available countries in the waterbase database.
    
    Returns:
        JSON response with available countries
    """
    try:
        response = eea_service.get_available_countries()
        
        countries = []
        if "results" in response:
            for record in response["results"]:
                countries.append({
                    "code": record.get("countryCode")
                })
        
        return {
            "success": True,
            "data": countries,
            "metadata": {
                "total_countries": len(countries)
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch countries list: {str(e)}"
        )


@app.get("/waterbase/parameters")
async def get_parameters(
    country: Optional[str] = Query(None, description="Filter by country code")
) -> Dict[str, Any]:
    """
    Get list of water parameters, optionally filtered by country.
    
    Args:
        country: Optional country code filter
        
    Returns:
        JSON response with parameters data
    """
    try:
        response = eea_service.get_available_parameters(country)
        
        parameters = []
        if "results" in response:
            for record in response["results"]:
                parameters.append({
                    "code": record.get("parameterWaterCategoryCode")
                })
        
        return {
            "success": True,
            "filters": {"country": country},
            "data": parameters,
            "metadata": {
                "total_parameters": len(parameters)
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch parameters list: {str(e)}"
        )


@app.get("/waterbase/summary")
async def get_waterbase_summary(
    country: Optional[str] = Query(None, description="Filter by country code")
) -> Dict[str, Any]:
    """
    Get summary statistics of waterbase data.
    
    Args:
        country: Optional country code filter
        
    Returns:
        JSON response with summary statistics
    """
    try:
        # Get data as DataFrame for analysis using service method
        df = eea_service.get_waterbase_summary_dataframe(country)
        
        if df.empty:
            return {
                "success": True,
                "filters": {"country": country},
                "data": {
                    "total_records": 0,
                    "countries": 0,
                    "parameters": 0,
                    "years_covered": 0
                }
            }
        
        summary = {
            "total_records": len(df),
            "countries": df["countryCode"].nunique() if "countryCode" in df.columns else 0,
            "parameters": df["parameterWaterCategoryCode"].nunique() if "parameterWaterCategoryCode" in df.columns else 0
        }
        
        # Add year coverage
        if "phenomenonTimeReferenceYear" in df.columns:
            years = df["phenomenonTimeReferenceYear"].dropna()
            if len(years) > 0:
                summary["years_covered"] = {
                    "min_year": int(years.min()),
                    "max_year": int(years.max()),
                    "total_years": int(years.nunique())
                }
        
        # Add country breakdown if not filtered by country
        if not country and "countryCode" in df.columns:
            country_counts = df["countryCode"].value_counts().head(10).to_dict()
            summary["top_countries"] = country_counts
        
        # Add parameter breakdown
        if "parameterWaterCategoryCode" in df.columns:
            param_counts = df["parameterWaterCategoryCode"].value_counts().head(10).to_dict()
            summary["top_parameters"] = param_counts
        
        return {
            "success": True,
            "filters": {"country": country},
            "data": summary
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate waterbase summary: {str(e)}"
        )




def start_server(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI server."""
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    start_server()