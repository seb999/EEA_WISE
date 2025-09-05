#!/usr/bin/env python3
"""
Example usage of the EEA Water Data API Service using Dremio.
"""

from src.dremio_service import DremioApiService
import pandas as pd


def flatten_dremio_data(dremio_result):
    """Transform Dremio's nested format into flat dictionaries."""
    if not dremio_result.get("rows") or not dremio_result.get("columns"):
        return []
    
    columns = dremio_result["columns"]
    rows = dremio_result["rows"]
    flattened_data = []
    
    for row_data in rows:
        if isinstance(row_data, dict) and "row" in row_data:
            row_values = row_data["row"]
            flattened_row = {}
            
            for i, col_info in enumerate(columns):
                col_name = col_info.get("name", f"col_{i}")
                if i < len(row_values):
                    value_obj = row_values[i]
                    if isinstance(value_obj, dict) and "v" in value_obj:
                        flattened_row[col_name] = value_obj["v"]
                    else:
                        flattened_row[col_name] = value_obj
                else:
                    flattened_row[col_name] = None
                    
            flattened_data.append(flattened_row)
        elif isinstance(row_data, list):
            flattened_row = {}
            for i, col_info in enumerate(columns):
                col_name = col_info.get("name", f"col_{i}")
                if i < len(row_data):
                    flattened_row[col_name] = row_data[i]
                else:
                    flattened_row[col_name] = None
                    
            flattened_data.append(flattened_row)
    
    return flattened_data


def example_custom_query():
    """Example of executing a custom SQL query."""
    dremio_service = DremioApiService()
    
    # Custom query to get waterbase data for Germany
    custom_query = """
    SELECT * FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_T_WISE6_AggregatedData"
    WHERE countryCode = 'DE'
    LIMIT 20
    """
    
    print("Custom Query Example:")
    print("Getting waterbase data for Germany...")
    
    try:
        result = dremio_service.execute_query(custom_query, limit=20)
        flattened_data = flatten_dremio_data(result)
        
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            print(f"Retrieved {len(df)} records")
            print("\nSample data:")
            print(df.head())
            return df
        else:
            print("No data returned")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def example_waterbase_data():
    """Example of getting waterbase data."""
    dremio_service = DremioApiService()
    
    print("\nWaterbase Data Example:")
    print("Fetching waterbase data for France...")
    
    try:
        result = dremio_service.get_waterbase_aggregated_data("FR", 10)
        flattened_data = flatten_dremio_data(result)
        
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            print(f"Retrieved {len(df)} records")
            print("\nSample data:")
            print(df.head())
            return df
        else:
            print("No data returned")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def example_disaggregated_data():
    """Example of getting disaggregated waterbase data."""
    dremio_service = DremioApiService()
    
    print("\nDisaggregated Data Example:")
    print("Getting disaggregated waterbase data for Denmark...")
    
    try:
        result = dremio_service.get_waterbase_disaggregated_data("DK", 15)
        flattened_data = flatten_dremio_data(result)
        
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            print(f"Retrieved DataFrame with {len(df)} rows and {len(df.columns)} columns")
            print("\nColumn names:")
            print(df.columns.tolist())
            print("\nSample data:")
            print(df.head())
            return df
        else:
            print("No data returned")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def main():
    """Run all examples."""
    print("EEA Water Data API Service Examples")
    print("=" * 50)
    
    # Example 1: Custom SQL query
    df1 = example_custom_query()
    
    # Example 2: Waterbase aggregated data
    df2 = example_waterbase_data()
    
    # Example 3: Waterbase disaggregated data
    df3 = example_disaggregated_data()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    
    if df1 is not None or df2 is not None or df3 is not None:
        print("All examples returned data successfully")
    else:
        print("No data was retrieved. Check your Dremio connection.")


if __name__ == "__main__":
    main()