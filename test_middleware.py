"""
Test script to verify middleware connectivity and query execution.
"""
from dotenv import load_dotenv

# Reload environment variables
load_dotenv(override=True)

from src.dremio_service import DremioApiService

def test_middleware_connection():
    """Test middleware service initialization and query execution."""

    print("=" * 60)
    print("MIDDLEWARE CONNECTION TEST")
    print("=" * 60)

    # Initialize service
    print("\n1. Initializing DremioApiService...")
    service = DremioApiService()

    # Check service info
    print("\n2. Service Information:")
    info = service.get_service_info()
    for key, value in info.items():
        print(f"   {key}: {value}")

    # Test simple query
    print("\n3. Testing simple COUNT query...")
    test_query = """
    SELECT COUNT(*) as total_records
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
    LIMIT 1
    """

    try:
        result = service.execute_query(test_query)
        print("   [OK] Query executed successfully!")
        print(f"   Response keys: {list(result.keys())}")

        # Check if we have data
        if 'rows' in result and result['rows']:
            print(f"   [OK] Received {len(result['rows'])} rows")
        elif isinstance(result, dict):
            print(f"   Response preview: {str(result)[:200]}...")

    except Exception as e:
        print(f"   [ERROR] Query failed: {e}")
        return False

    # Test monitoring sites query
    print("\n4. Testing monitoring sites query...")
    sites_query = """
    SELECT
        thematicIdIdentifier,
        monitoringSiteName,
        countryCode,
        lat,
        lon
    FROM "Local S3"."datahub-pre-01".discodata."WISE_SOE".latest."Waterbase_S_WISE_SpatialObject_DerivedData"
    WHERE countryCode = 'FR'
    LIMIT 5
    """

    try:
        result = service.execute_query(sites_query)
        print("   [OK] Sites query executed successfully!")

        if 'rows' in result:
            print(f"   [OK] Received {len(result['rows'])} monitoring sites")
        elif isinstance(result, dict):
            print(f"   Response preview: {str(result)[:200]}...")

    except Exception as e:
        print(f"   [ERROR] Sites query failed: {e}")
        return False

    print("\n" + "=" * 60)
    print("[SUCCESS] ALL TESTS PASSED - Middleware is working correctly!")
    print("=" * 60)

    return True

if __name__ == "__main__":
    success = test_middleware_connection()
    exit(0 if success else 1)
