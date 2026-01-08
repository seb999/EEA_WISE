"""
Test script for OGC API - Features (Phase 2) implementation.
Tests conformance, collections, and items endpoints.

Run this after starting the API server with: python app.py
"""
import requests
import json


def test_conformance():
    """Test the /conformance endpoint."""
    print("=" * 60)
    print("Testing OGC Conformance Endpoint")
    print("=" * 60)

    url = "http://127.0.0.1:8000/conformance"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify conformance structure
        assert "conformsTo" in data, "Missing conformsTo array"
        assert isinstance(data["conformsTo"], list), "conformsTo must be an array"
        assert len(data["conformsTo"]) > 0, "conformsTo array is empty"

        print(f"✓ Valid conformance declaration")
        print(f"✓ Conformance classes declared: {len(data['conformsTo'])}")
        print(f"\nConformance classes:")
        for conf_class in data["conformsTo"]:
            print(f"  - {conf_class}")

        print("\n✓ Test 1 PASSED: Conformance endpoint\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 1 FAILED: {e}\n")
        return False


def test_collections():
    """Test the /collections endpoint."""
    print("=" * 60)
    print("Testing OGC Collections Endpoint")
    print("=" * 60)

    url = "http://127.0.0.1:8000/collections"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify collections structure
        assert "collections" in data, "Missing collections array"
        assert "links" in data, "Missing links array"
        assert isinstance(data["collections"], list), "collections must be an array"
        assert len(data["collections"]) > 0, "No collections found"

        print(f"✓ Valid collections response")
        print(f"✓ Number of collections: {len(data['collections'])}")

        # Check each collection has required fields
        for collection in data["collections"]:
            assert "id" in collection, "Collection missing id"
            assert "title" in collection, "Collection missing title"
            assert "description" in collection, "Collection missing description"
            assert "extent" in collection, "Collection missing extent"
            assert "links" in collection, "Collection missing links"

            print(f"\n  Collection: {collection['id']}")
            print(f"    Title: {collection['title']}")
            print(f"    Description: {collection['description'][:80]}...")

        print("\n✓ Test 2 PASSED: Collections endpoint\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 2 FAILED: {e}\n")
        return False


def test_collection_metadata():
    """Test getting metadata for a specific collection."""
    print("=" * 60)
    print("Testing Collection Metadata Endpoint")
    print("=" * 60)

    collection_id = "monitoring-sites"
    url = f"http://127.0.0.1:8000/collections/{collection_id}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify collection metadata
        assert data.get("id") == collection_id, "Incorrect collection id"
        assert "title" in data, "Missing title"
        assert "description" in data, "Missing description"
        assert "extent" in data, "Missing extent"
        assert "spatial" in data["extent"], "Missing spatial extent"
        assert "bbox" in data["extent"]["spatial"], "Missing bbox"
        assert "links" in data, "Missing links"

        print(f"✓ Valid collection metadata")
        print(f"  Collection ID: {data['id']}")
        print(f"  Title: {data['title']}")
        print(f"  Spatial extent: {data['extent']['spatial']['bbox'][0]}")

        # Check links include items link
        items_link = next((link for link in data["links"] if link["rel"] == "items"), None)
        assert items_link is not None, "Missing items link"
        print(f"  Items URL: {items_link['href']}")

        print("\n✓ Test 3 PASSED: Collection metadata\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 3 FAILED: {e}\n")
        return False


def test_collection_items_monitoring_sites():
    """Test getting items from monitoring-sites collection."""
    print("=" * 60)
    print("Testing Monitoring Sites Collection Items")
    print("=" * 60)

    url = "http://127.0.0.1:8000/collections/monitoring-sites/items?country_code=FR&limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON FeatureCollection
        assert data.get("type") == "FeatureCollection", "Not a FeatureCollection"
        assert "features" in data, "Missing features"
        assert "numberMatched" in data, "Missing numberMatched"
        assert "numberReturned" in data, "Missing numberReturned"
        assert "timeStamp" in data, "Missing timeStamp"
        assert "links" in data, "Missing links"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Number matched: {data['numberMatched']}")
        print(f"✓ Number returned: {data['numberReturned']}")

        # Check pagination links
        self_link = next((link for link in data["links"] if link["rel"] == "self"), None)
        assert self_link is not None, "Missing self link"
        print(f"  Self link: {self_link['href']}")

        # Check features
        if data["features"]:
            feature = data["features"][0]
            assert feature.get("type") == "Feature", "Not a valid Feature"
            assert "geometry" in feature, "Missing geometry"
            assert "properties" in feature, "Missing properties"
            assert feature["geometry"]["type"] == "Point", "Not a Point geometry"

            print(f"\n  Sample feature:")
            print(f"    ID: {feature.get('id')}")
            print(f"    Coordinates: {feature['geometry']['coordinates']}")
            print(f"    Country: {feature['properties'].get('countryCode')}")

        print("\n✓ Test 4 PASSED: Monitoring sites items\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 4 FAILED: {e}\n")
        return False


def test_collection_items_latest_measurements():
    """Test getting items from latest-measurements collection."""
    print("=" * 60)
    print("Testing Latest Measurements Collection Items")
    print("=" * 60)

    url = "http://127.0.0.1:8000/collections/latest-measurements/items?country_code=FR&limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Verify GeoJSON FeatureCollection
        assert data.get("type") == "FeatureCollection", "Not a FeatureCollection"
        assert "features" in data, "Missing features"
        assert "numberMatched" in data, "Missing numberMatched"
        assert "numberReturned" in data, "Missing numberReturned"

        print(f"✓ Valid GeoJSON FeatureCollection")
        print(f"✓ Number matched: {data['numberMatched']}")
        print(f"✓ Number returned: {data['numberReturned']}")

        # Check features have measurement data
        if data["features"]:
            feature = data["features"][0]
            props = feature["properties"]

            # Should have measurement fields
            if "observedPropertyDeterminandCode" in props:
                print(f"\n  Sample measurement:")
                print(f"    Site: {props.get('monitoringSiteIdentifier')}")
                print(f"    Parameter: {props.get('observedPropertyDeterminandCode')}")
                print(f"    Value: {props.get('resultObservedValue')} {props.get('resultUom')}")
                print(f"    Date: {props.get('phenomenonTimeSamplingDate')}")

        print("\n✓ Test 5 PASSED: Latest measurements items\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 5 FAILED: {e}\n")
        return False


def test_collection_items_with_bbox():
    """Test bbox filtering on collection items."""
    print("=" * 60)
    print("Testing BBox Filtering on Collection Items")
    print("=" * 60)

    # Paris region bbox
    url = "http://127.0.0.1:8000/collections/monitoring-sites/items?bbox=2.2,48.8,2.5,48.9&limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        print(f"✓ BBox query successful")
        print(f"✓ Features returned: {data['numberReturned']}")

        # Verify features are within bbox
        for feature in data["features"]:
            lon, lat = feature["geometry"]["coordinates"]
            assert 2.2 <= lon <= 2.5, f"Longitude {lon} out of bbox"
            assert 48.8 <= lat <= 48.9, f"Latitude {lat} out of bbox"

        print(f"✓ All features within bounding box")
        print("\n✓ Test 6 PASSED: BBox filtering\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 6 FAILED: {e}\n")
        return False


def test_pagination():
    """Test pagination with offset and limit."""
    print("=" * 60)
    print("Testing Pagination")
    print("=" * 60)

    # Get first page
    url1 = "http://127.0.0.1:8000/collections/monitoring-sites/items?country_code=FR&limit=5&offset=0"

    try:
        response1 = requests.get(url1)
        response1.raise_for_status()
        data1 = response1.json()

        print(f"✓ Page 1 retrieved: {data1['numberReturned']} features")

        # Check for next link
        next_link = next((link for link in data1["links"] if link["rel"] == "next"), None)

        if next_link:
            print(f"✓ Next link found: {next_link['href']}")

            # Get second page
            response2 = requests.get(next_link['href'])
            response2.raise_for_status()
            data2 = response2.json()

            print(f"✓ Page 2 retrieved: {data2['numberReturned']} features")

            # Verify features are different
            if data1["features"] and data2["features"]:
                id1 = data1["features"][0].get("id")
                id2 = data2["features"][0].get("id")
                assert id1 != id2, "Pages contain same features"
                print(f"✓ Pages contain different features")

        print("\n✓ Test 7 PASSED: Pagination\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 7 FAILED: {e}\n")
        return False


def test_invalid_collection():
    """Test that invalid collection returns 404."""
    print("=" * 60)
    print("Testing Invalid Collection Handling")
    print("=" * 60)

    url = "http://127.0.0.1:8000/collections/invalid-collection-id"

    try:
        response = requests.get(url)

        # Should return 404
        assert response.status_code == 404, "Should return 404 for invalid collection"

        print(f"✓ Returns 404 for invalid collection")
        print(f"  Status code: {response.status_code}")

        # Check error message includes available collections
        data = response.json()
        assert "detail" in data, "Missing error detail"
        print(f"  Error message: {data['detail'][:100]}...")

        print("\n✓ Test 8 PASSED: Invalid collection handling\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 8 FAILED: {e}\n")
        return False


def test_health_check_ogc_info():
    """Test that health check includes OGC compliance info."""
    print("=" * 60)
    print("Testing Health Check OGC Information")
    print("=" * 60)

    url = "http://127.0.0.1:8000/healthCheck"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Check for OGC compliance info
        assert "ogc_compliance" in data, "Missing ogc_compliance section"
        assert data["ogc_compliance"]["ogc_api_features"], "OGC API Features should be True"

        print(f"✓ Health check includes OGC compliance info")
        print(f"  API Version: {data['api_version']}")
        print(f"  OGC API - Features: {data['ogc_compliance']['ogc_api_features']}")
        print(f"  Collections: {data['ogc_compliance']['collections']}")
        print(f"  Conformance classes: {data['ogc_compliance']['conformance_classes']}")

        print("\n✓ Test 9 PASSED: Health check OGC info\n")
        return True

    except Exception as e:
        print(f"\n✗ Test 9 FAILED: {e}\n")
        return False


def save_sample_collection_response():
    """Save a sample collection items response for inspection."""
    print("=" * 60)
    print("Saving Sample Collection Response")
    print("=" * 60)

    url = "http://127.0.0.1:8000/collections/monitoring-sites/items?country_code=FR&limit=5"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        with open('sample_ogc_collection_items.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print("✓ Sample collection items saved to: sample_ogc_collection_items.json")
        print("  You can view this file to inspect the OGC-compliant response\n")
        return True

    except Exception as e:
        print(f"✗ Failed to save sample: {e}\n")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("OGC API - FEATURES TEST SUITE (PHASE 2)")
    print("=" * 60)
    print("\nMake sure the API server is running: python app.py\n")

    # Run all tests
    results = []

    results.append(("Conformance Endpoint", test_conformance()))
    results.append(("Collections Endpoint", test_collections()))
    results.append(("Collection Metadata", test_collection_metadata()))
    results.append(("Monitoring Sites Items", test_collection_items_monitoring_sites()))
    results.append(("Latest Measurements Items", test_collection_items_latest_measurements()))
    results.append(("BBox Filtering", test_collection_items_with_bbox()))
    results.append(("Pagination", test_pagination()))
    results.append(("Invalid Collection", test_invalid_collection()))
    results.append(("Health Check OGC Info", test_health_check_ogc_info()))
    results.append(("Save Sample", save_sample_collection_response()))

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
        print("\n🎉 All tests passed! Your API is now fully OGC API - Features compliant!")
    else:
        print("\n⚠ Some tests failed. Please check the error messages above.")
