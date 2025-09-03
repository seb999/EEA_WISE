#!/usr/bin/env python3
"""
Example usage of the EEA API Service with different query patterns.
"""

from src.eea_api_service import EEAApiService
import pandas as pd


def example_custom_query():
    """Example of executing a custom SQL query."""
    api_service = EEAApiService()
    
    # Custom query to get data for a specific country
    custom_query = """
    SELECT TOP 20 countryCode, countryName, gwbName, pollutantCode, pollutantName, statusValue 
    FROM [WISE_WFD].[latest].[GWB_GroundWaterBody_GWPollutant] 
    WHERE countryCode = 'DE'
    """
    
    print("🔍 Custom Query Example:")
    print("Getting groundwater data for Germany...")
    
    try:
        df = api_service.query_to_dataframe(custom_query, hits_per_page=20)
        print(f"✅ Retrieved {len(df)} records")
        print("\n📋 Sample data:")
        print(df[['countryName', 'gwbName', 'pollutantName', 'statusValue']].head())
        return df
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def example_pagination():
    """Example of working with pagination."""
    api_service = EEAApiService()
    
    print("\n🔄 Pagination Example:")
    print("Fetching multiple pages of data...")
    
    all_data = []
    
    for page in range(1, 4):  # Get first 3 pages
        try:
            data = api_service.get_groundwater_pollutant_data(
                limit=10, 
                page=page, 
                hits_per_page=10
            )
            
            if 'results' in data and data['results']:
                all_data.extend(data['results'])
                print(f"📄 Page {page}: {len(data['results'])} records")
            else:
                print(f"📄 Page {page}: No data")
                break
                
        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            break
    
    if all_data:
        df = pd.DataFrame(all_data)
        print(f"✅ Total records collected: {len(df)}")
        print(f"📊 Unique countries: {df['countryCode'].nunique()}")
        return df
    
    return None


def example_data_analysis():
    """Example of basic data analysis."""
    api_service = EEAApiService()
    
    print("\n📊 Data Analysis Example:")
    
    try:
        # Get a larger dataset
        df = api_service.get_groundwater_pollutant_dataframe(limit=50, hits_per_page=50)
        
        if df.empty:
            print("❌ No data available for analysis")
            return
            
        print(f"📈 Dataset shape: {df.shape}")
        
        # Basic statistics
        print(f"\n🌍 Countries in dataset: {df['countryCode'].nunique()}")
        print("Top 5 countries by record count:")
        country_counts = df['countryCode'].value_counts().head()
        for country, count in country_counts.items():
            country_name = df[df['countryCode'] == country]['countryName'].iloc[0]
            print(f"  {country} ({country_name}): {count} records")
        
        # Pollutant analysis
        if 'pollutantName' in df.columns:
            print(f"\n🧪 Unique pollutants: {df['pollutantName'].nunique()}")
            print("Top 5 pollutants:")
            pollutant_counts = df['pollutantName'].value_counts().head()
            for pollutant, count in pollutant_counts.items():
                print(f"  {pollutant}: {count} records")
        
        # Status analysis
        if 'statusValue' in df.columns:
            print(f"\n📊 Status distribution:")
            status_counts = df['statusValue'].value_counts()
            for status, count in status_counts.items():
                print(f"  {status}: {count} records")
                
    except Exception as e:
        print(f"❌ Analysis error: {e}")


def main():
    """Run all examples."""
    print("🚀 EEA API Service - Advanced Examples")
    print("=" * 50)
    
    # Run examples
    example_custom_query()
    example_pagination()
    example_data_analysis()
    
    print("\n✨ Examples completed!")


if __name__ == "__main__":
    main()