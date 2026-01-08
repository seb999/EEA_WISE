"""
Test script to verify GeoJSON output format for OGC compliance.
Run this after starting the API server with: python app.py
"""
import requests
import json


def test_ogc_spatial_locations():
    """Test the new OGC-compliant spatial locations endpoint."""
    print("=" * 60)
    print("Testing OGC Spatial Locations Endpoint")
    print("=" * 60)

    # Test 1: Basic spatial locations query
    print("\n1. Testing basic spatial locations (France, limit 5)...")
    url = "http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=5"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON structure
        assert data.get("type") == "FeatureCollection", "Missing FeatureCollection type"
        assert "features" in data, "Missing features array"
        assert "numberMatched" in data, "Missing numberMatched"
        assert "numberReturned" in data, "Missing numberReturned"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Check first feature structure
        if data["features"]:
            feature = data["features"][0]
            assert feature.get("type") == "Feature", "Invalid feature type"
            assert "geometry" in feature, "Missing geometry"
            assert feature["geometry"]["type"] == "Point", "Invalid geometry type"
            assert "coordinates" in feature["geometry"], "Missing coordinates"
            assert len(feature["geometry"]["coordinates"]) == 2, "Invalid coordinates format"
            assert "properties" in feature, "Missing properties"

            print(f"✓ Valid Feature structure")
            print(f"  - ID: {feature.get('id')}")
            print(f"  - Coordinates: {feature['geometry']['coordinates']}")
            print(f"  - Country: {feature['properties'].get('countryCode')}")

        print("\n✓ Test 1 PASSED: Basic spatial locations query\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 1 FAILED: {e}\n")
        return False


def test_bbox_filter():
    """Test bounding box filtering."""
    print("=" * 60)
    print("Testing Bounding Box Filter")
    print("=" * 60)

    # Test 2: Bounding box query (France region)
    print("\n2. Testing bounding box filter (France region)...")
    # bbox: Paris region approximately
    url = "http://127.0.0.1:8000/ogc/spatial-locations?bbox=1.0,48.0,3.0,49.5&limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        print(f"✓ Bounding box query successful")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Verify coordinates are within bbox
        for feature in data["features"]:
            lon, lat = feature["geometry"]["coordinates"]
            assert 1.0 <= lon <= 3.0, f"Longitude {lon} out of bbox"
            assert 48.0 <= lat <= 49.5, f"Latitude {lat} out of bbox"

        print(f"✓ All coordinates within bounding box")
        print("\n✓ Test 2 PASSED: Bounding box filter\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 2 FAILED: {e}\n")
        return False


def test_waterbase_geojson():
    """Test waterbase data with GeoJSON format."""
    print("=" * 60)
    print("Testing Waterbase Data with GeoJSON Format")
    print("=" * 60)

    # Test 3: Waterbase data in GeoJSON format
    print("\n3. Testing waterbase data with GeoJSON format...")
    url = "http://127.0.0.1:8000/waterbase?country_code=FR&limit=5&format=geojson"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON structure
        assert data.get("type") == "FeatureCollection", "Missing FeatureCollection type"
        assert "features" in data, "Missing features array"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Check feature properties include measurement data
        if data["features"]:
            feature = data["features"][0]
            props = feature["properties"]

            # Should have water quality measurement fields
            expected_fields = ["observedPropertyDeterminandCode", "resultObservedValue"]
            for field in expected_fields:
                if field in props:
                    print(f"  ✓ Has measurement field: {field}")

            print(f"\n  Sample measurement:")
            print(f"  - Site: {props.get('monitoringSiteIdentifier')}")
            print(f"  - Parameter: {props.get('observedPropertyDeterminandCode')}")
            print(f"  - Value: {props.get('resultObservedValue')}")

        print("\n✓ Test 3 PASSED: Waterbase GeoJSON format\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 3 FAILED: {e}\n")
        return False


def test_ogc_spatial_with_country():
    """Test OGC spatial locations endpoint with country filter."""
    print("=" * 60)
    print("Testing OGC Spatial Locations with Country Filter")
    print("=" * 60)

    # Test 4: OGC spatial locations with country filter
    print("\n4. Testing OGC spatial locations with country filter (Germany)...")
    url = "http://127.0.0.1:8000/ogc/spatial-locations?country_code=DE&limit=5"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON structure
        assert data.get("type") == "FeatureCollection", "Missing FeatureCollection type"
        assert "features" in data, "Missing features array"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Verify all features are from Germany
        for feature in data["features"]:
            country = feature["properties"].get("countryCode")
            assert country == "DE", f"Expected DE, got {country}"

        print(f"✓ All features are from Germany")
        print("\n✓ Test 4 PASSED: OGC spatial with country filter\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 4 FAILED: {e}\n")
        return False


def test_site_measurements_geojson():
    """Test site-specific measurements endpoint with GeoJSON format."""
    print("=" * 60)
    print("Testing Site Measurements with GeoJSON")
    print("=" * 60)

    # First, get a site identifier
    print("\n5. Getting a sample site identifier...")
    sites_url = "http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=1"

    try:
        sites_response = requests.get(sites_url)
        sites_response.raise_for_status()
        sites_data = sites_response.json()

        if not sites_data.get("features"):
            print("⚠ No sites found, skipping test")
            return True

        site_id = sites_data["features"][0].get("id")
        print(f"  Using site: {site_id}")

        # Test site measurements with GeoJSON
        print(f"\n  Testing measurements for site {site_id} with GeoJSON format...")
        url = f"http://127.0.0.1:8000/waterbase/site/{site_id}?format=geojson"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON structure
        assert data.get("type") == "FeatureCollection", "Missing FeatureCollection type"
        assert "features" in data, "Missing features array"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Verify features have measurement properties
        if data["features"]:
            feature = data["features"][0]
            props = feature["properties"]

            if "observedPropertyDeterminandCode" in props:
                print(f"  ✓ Has parameter: {props.get('observedPropertyDeterminandCode')}")
            if "resultObservedValue" in props:
                print(f"  ✓ Has value: {props.get('resultObservedValue')}")

        print("\n✓ Test 5 PASSED: Site measurements GeoJSON format\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 5 FAILED: {e}\n")
        return False


def test_country_measurements_geojson():
    """Test country-specific measurements endpoint with GeoJSON format."""
    print("=" * 60)
    print("Testing Country Measurements with GeoJSON")
    print("=" * 60)

    # Test 6: Country measurements with GeoJSON
    print("\n6. Testing latest measurements by country with GeoJSON format...")
    url = "http://127.0.0.1:8000/waterbase/country/FR?format=geojson"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON structure
        assert data.get("type") == "FeatureCollection", "Missing FeatureCollection type"
        assert "features" in data, "Missing features array"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Verify all features are from France
        for feature in data["features"]:
            country = feature["properties"].get("countryCode")
            assert country == "FR", f"Expected FR, got {country}"

        print(f"✓ All features are from France")
        print("\n✓ Test 6 PASSED: Country measurements GeoJSON format\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 6 FAILED: {e}\n")
        return False


def save_sample_geojson():
    """Save a sample GeoJSON output for inspection."""
    print("=" * 60)
    print("Saving Sample GeoJSON")
    print("=" * 60)

    url = "http://127.0.0.1:8000/ogc/spatial-locations?country_code=FR&limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        with open('sample_geojson_output.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print("✓ Sample GeoJSON saved to: sample_geojson_output.json")
        print("  You can view this in QGIS, geojson.io, or any GIS tool\n")
        return True

    except Exception as e:
        print(f"✗ Failed to save sample: {e}\n")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("OGC COMPLIANCE TEST SUITE")
    print("=" * 60)
    print("\nMake sure the API server is running: python app.py\n")

    # Run all tests
    results = []

    results.append(("OGC Spatial Locations", test_ogc_spatial_locations()))
    results.append(("Bounding Box Filter", test_bbox_filter()))
    results.append(("Waterbase GeoJSON", test_waterbase_geojson()))
    results.append(("OGC Spatial with Country", test_ogc_spatial_with_country()))
    results.append(("Site Measurements GeoJSON", test_site_measurements_geojson()))
    results.append(("Country Measurements GeoJSON", test_country_measurements_geojson()))
    results.append(("Save Sample", save_sample_geojson()))

    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Your API is now OGC-compliant for spatial data.")
    else:
        print("\n⚠ Some tests failed. Please check the error messages above.")
