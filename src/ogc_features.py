"""
OGC API - Features implementation module.

This module provides classes and utilities for implementing OGC API - Features
specification (formerly WFS 3.0) for the EEA WISE water quality data API.

References:
- OGC API - Features: https://ogcapi.ogc.org/features/
- Part 1 Core: http://www.opengis.net/doc/IS/ogcapi-features-1/1.0
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import urljoin


class OGCConformance:
    """
    OGC API - Features conformance classes declaration.

    This class defines which OGC conformance classes the API implements.
    """

    # OGC API - Features Part 1: Core conformance classes
    CORE = "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core"
    GEOJSON = "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson"
    HTML = "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html"
    OAS30 = "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30"

    # OGC API - Features Part 2: CRS by reference
    CRS = "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs"

    # Additional conformance classes
    BBOX = "http://www.opengis.net/spec/ogcapi-features-1/1.0/req/core/bbox"

    @classmethod
    def get_conformance_declaration(cls) -> Dict[str, List[str]]:
        """
        Get the conformance declaration for this API.

        Returns:
            Dictionary with conformsTo array listing implemented conformance classes
        """
        return {
            "conformsTo": [
                cls.CORE,
                cls.GEOJSON,
                cls.OAS30,
                cls.BBOX
            ]
        }


class OGCCollection:
    """
    Represents an OGC API - Features collection.

    A collection is a dataset that can be queried via the /collections/{id}/items endpoint.
    """

    def __init__(
        self,
        id: str,
        title: str,
        description: str,
        extent_spatial: Optional[List[float]] = None,
        extent_temporal: Optional[List[str]] = None,
        item_type: str = "feature",
        crs: List[str] = None
    ):
        """
        Initialize an OGC collection.

        Args:
            id: Unique collection identifier
            title: Human-readable title
            description: Detailed description
            extent_spatial: Bounding box [minLon, minLat, maxLon, maxLat]
            extent_temporal: Temporal extent [start, end] in ISO 8601 format
            item_type: Type of items (default: "feature")
            crs: List of supported coordinate reference systems
        """
        self.id = id
        self.title = title
        self.description = description
        self.extent_spatial = extent_spatial or [-180, -90, 180, 90]
        self.extent_temporal = extent_temporal
        self.item_type = item_type
        self.crs = crs or ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]

    def to_dict(self, base_url: str = "") -> Dict[str, Any]:
        """
        Convert collection to OGC-compliant dictionary.

        Args:
            base_url: Base URL for generating links

        Returns:
            Dictionary representation of the collection
        """
        collection = {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "itemType": self.item_type,
            "crs": self.crs,
            "extent": {
                "spatial": {
                    "bbox": [self.extent_spatial],
                    "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                }
            },
            "links": [
                {
                    "href": f"{base_url}/collections/{self.id}",
                    "rel": "self",
                    "type": "application/json",
                    "title": "This collection"
                },
                {
                    "href": f"{base_url}/collections/{self.id}/items",
                    "rel": "items",
                    "type": "application/geo+json",
                    "title": "Items in this collection"
                },
                {
                    "href": f"{base_url}/collections/{self.id}?f=html",
                    "rel": "alternate",
                    "type": "text/html",
                    "title": "This collection as HTML"
                }
            ]
        }

        # Add temporal extent if available
        if self.extent_temporal:
            collection["extent"]["temporal"] = {
                "interval": [self.extent_temporal],
                "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
            }

        return collection


class OGCCollections:
    """
    Manages OGC API - Features collections for the EEA WISE API.
    """

    def __init__(self):
        """Initialize the collections manager with EEA WISE collections."""
        self.collections = {}
        self._initialize_collections()

    def _initialize_collections(self):
        """Initialize the available collections."""

        # Collection 1: Monitoring Sites
        self.collections["monitoring-sites"] = OGCCollection(
            id="monitoring-sites",
            title="Water Quality Monitoring Sites",
            description=(
                "Locations of water quality monitoring sites across Europe. "
                "Each site represents a location where water quality measurements are taken. "
                "Includes site identifiers, names, and geographic coordinates."
            ),
            extent_spatial=[-31.5, 27.6, 69.1, 81.0],  # Europe bounding box
            extent_temporal=["1990-01-01T00:00:00Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")]
        )

        # Collection 2: Latest Measurements
        self.collections["latest-measurements"] = OGCCollection(
            id="latest-measurements",
            title="Latest Water Quality Measurements",
            description=(
                "Most recent water quality measurement for each parameter at each monitoring site. "
                "Includes chemical parameter values, units of measurement, and sampling dates. "
                "This collection provides a snapshot of current water quality conditions."
            ),
            extent_spatial=[-31.5, 27.6, 69.1, 81.0],
            extent_temporal=["1990-01-01T00:00:00Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")]
        )

        # Collection 3: Disaggregated Data
        self.collections["disaggregated-data"] = OGCCollection(
            id="disaggregated-data",
            title="Disaggregated Water Quality Data",
            description=(
                "Complete disaggregated water quality measurement data from the EEA Waterbase. "
                "Includes all historical measurements with full metadata, chemical parameters, "
                "observed values, quality flags, and temporal information."
            ),
            extent_spatial=[-31.5, 27.6, 69.1, 81.0],
            extent_temporal=["1990-01-01T00:00:00Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")]
        )

        # Collection 4: Time Series
        self.collections["time-series"] = OGCCollection(
            id="time-series",
            title="Water Quality Time Series",
            description=(
                "Time-series data for water quality parameters at monitoring sites. "
                "Supports raw, monthly, and yearly aggregations. "
                "Useful for trend analysis and temporal pattern identification."
            ),
            extent_spatial=[-31.5, 27.6, 69.1, 81.0],
            extent_temporal=["1990-01-01T00:00:00Z", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")]
        )

    def get_collection(self, collection_id: str) -> Optional[OGCCollection]:
        """
        Get a specific collection by ID.

        Args:
            collection_id: Collection identifier

        Returns:
            OGCCollection instance or None if not found
        """
        return self.collections.get(collection_id)

    def get_all_collections(self, base_url: str = "") -> Dict[str, Any]:
        """
        Get all collections in OGC-compliant format.

        Args:
            base_url: Base URL for generating links

        Returns:
            Dictionary with collections array
        """
        return {
            "collections": [
                collection.to_dict(base_url)
                for collection in self.collections.values()
            ],
            "links": [
                {
                    "href": f"{base_url}/collections",
                    "rel": "self",
                    "type": "application/json",
                    "title": "This document"
                },
                {
                    "href": f"{base_url}/",
                    "rel": "service-desc",
                    "type": "application/json",
                    "title": "API definition"
                }
            ]
        }

    def list_collection_ids(self) -> List[str]:
        """
        Get list of all collection IDs.

        Returns:
            List of collection identifiers
        """
        return list(self.collections.keys())


class OGCLinks:
    """
    Helper class for generating OGC-compliant link objects.
    """

    @staticmethod
    def create_link(
        href: str,
        rel: str,
        type: str = "application/json",
        title: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Create an OGC-compliant link object.

        Args:
            href: URL of the linked resource
            rel: Relationship type (self, next, prev, items, etc.)
            type: Media type
            title: Optional title

        Returns:
            Link dictionary
        """
        link = {
            "href": href,
            "rel": rel,
            "type": type
        }
        if title:
            link["title"] = title
        return link

    @staticmethod
    def create_pagination_links(
        base_url: str,
        offset: int,
        limit: int,
        total: int,
        extra_params: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, str]]:
        """
        Create pagination links (self, next, prev).

        Args:
            base_url: Base URL for the current resource
            offset: Current offset
            limit: Current limit
            total: Total number of items
            extra_params: Additional query parameters

        Returns:
            List of link dictionaries
        """
        links = []

        # Build query string
        params = extra_params or {}

        # Self link
        self_params = {**params, "offset": str(offset), "limit": str(limit)}
        self_query = "&".join([f"{k}={v}" for k, v in self_params.items()])
        links.append(
            OGCLinks.create_link(
                f"{base_url}?{self_query}",
                "self",
                "application/geo+json",
                "This page"
            )
        )

        # Next link
        if offset + limit < total:
            next_offset = offset + limit
            next_params = {**params, "offset": str(next_offset), "limit": str(limit)}
            next_query = "&".join([f"{k}={v}" for k, v in next_params.items()])
            links.append(
                OGCLinks.create_link(
                    f"{base_url}?{next_query}",
                    "next",
                    "application/geo+json",
                    "Next page"
                )
            )

        # Previous link
        if offset > 0:
            prev_offset = max(0, offset - limit)
            prev_params = {**params, "offset": str(prev_offset), "limit": str(limit)}
            prev_query = "&".join([f"{k}={v}" for k, v in prev_params.items()])
            links.append(
                OGCLinks.create_link(
                    f"{base_url}?{prev_query}",
                    "prev",
                    "application/geo+json",
                    "Previous page"
                )
            )

        return links
