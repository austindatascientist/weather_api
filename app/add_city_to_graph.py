"""
Add a city to the weather graph.

This module provides a command-line interface to add cities to the Apache AGE
graph. It creates Temperature, Humidity, and Precipitation nodes with
relationship edges for the specified city.

Usage:
    # Using city name (geocoded to coordinates automatically)
    python -m app.add_city_to_graph "Denver"
    python -m app.add_city_to_graph "San Diego, CA"
    python -m app.add_city_to_graph "Seattle" --days 14

    # Using explicit coordinates (bypasses geocoding)
    python -m app.add_city_to_graph --lat 39.7392 --lon -104.9903 --city "Denver" --state "CO"

Note:
    This script calls the internal API endpoint to create the graph nodes.
    The API must be running for this script to work.
"""

import argparse
import sys
import requests

from .config import get_logger, API_BASE_URL
from .location_resolver import resolve_location, validate_coordinates

logger = get_logger(__name__)


def add_city_to_graph_by_name(city_name: str, days: int = 7):
    """
    Add a city to the graph by name (uses geocoding).

    Args:
        city_name: Name of the city (e.g., "Denver", "San Diego, CA")
        days: Number of days of historical data (default: 7)

    Returns:
        dict: Response from API with node/edge counts
    """
    # Resolve city name to coordinates
    try:
        logger.info(f"Resolving location: {city_name}...")
        location = resolve_location(city_name)
        logger.info(f"Found: {location.display_name}")
        logger.info(f"Coordinates: {location.latitude:.4f}, {location.longitude:.4f}")
    except ValueError as e:
        logger.error(f"✗ Geocoding error: {e}")
        sys.exit(1)

    # Determine state code
    state = location.state or "US"  # Default to "US" if state not extracted
    if len(state) != 2:
        logger.warning("Could not extract 2-letter state code, using 'US'")
        state = "US"

    # Call API to create nodes
    return add_city_to_graph_by_coordinates(
        city_name=location.city or city_name,
        state=state,
        latitude=location.latitude,
        longitude=location.longitude,
        days=days
    )


def add_city_to_graph_by_coordinates(city_name: str, state: str, latitude: float, longitude: float, days: int = 7):
    """
    Add a city to the graph using explicit coordinates.

    Args:
        city_name: Name of the city
        state: Two-letter state code (e.g., 'CO')
        latitude: Latitude coordinate
        longitude: Longitude coordinate
        days: Number of days of historical data (default: 7)

    Returns:
        dict: Response from API with node/edge counts
    """
    # Validate coordinates
    try:
        validate_coordinates(latitude, longitude)
    except ValueError as e:
        logger.error(f"✗ {e}")
        sys.exit(1)

    url = f"{API_BASE_URL}/api/graph/cities/nodes"

    payload = {
        "city_name": city_name,
        "state": state,
        "latitude": latitude,
        "longitude": longitude,
        "days": days
    }

    logger.info(f"Creating graph nodes for {city_name}, {state}...")

    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()

        logger.info(f"✓ Successfully created graph nodes for {city_name}")
        logger.info(f"  Distance to coast: {data['distance_to_coast_km']}km")
        logger.info(f"  Temperature nodes: {data['nodes_created']['temperature_nodes']}")
        logger.info(f"  Humidity nodes: {data['nodes_created']['humidity_nodes']}")
        logger.info(f"  Precipitation nodes: {data['nodes_created']['precipitation_nodes']}")
        logger.info(f"  Total edges: {sum(data['edges_created'].values())}")

        return data

    except requests.exceptions.ConnectionError as error:
        logger.error(f"✗ Could not connect to API at {API_BASE_URL}")
        logger.error("  Make sure the API is running with: make up")
        raise SystemExit(1) from error
    except requests.exceptions.HTTPError as error:
        logger.error(f"✗ API error: {error.response.status_code} - {error.response.text}")
        raise SystemExit(1) from error
    except requests.exceptions.Timeout as error:
        logger.error("✗ Request timed out - graph creation may take a while for large datasets")
        raise SystemExit(1) from error
    except Exception as error:
        logger.error(f"✗ Unexpected error: {error}")
        raise SystemExit(1) from error


def main():
    parser = argparse.ArgumentParser(
        description="Add a city to the weather graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using city name (automatic geocoding):
  python -m app.add_city_to_graph "Denver"
  python -m app.add_city_to_graph "San Diego, CA"
  python -m app.add_city_to_graph "Seattle" --days 14

  # Using explicit coordinates:
  python -m app.add_city_to_graph --lat 39.7392 --lon -104.9903 --city "Denver" --state "CO"
  python -m app.add_city_to_graph --lat 47.6062 --lon -122.3321 --city "Seattle" --state "WA" --days 14

Note:
  The API must be running for this script to work. Start it with: make up
        """
    )

    parser.add_argument("city_name", type=str, nargs='?', help="Name of the city (e.g., 'Denver', 'San Diego, CA')")
    parser.add_argument("--lat", "--latitude", type=float, dest="latitude", help="Latitude coordinate (requires --lon, --city, --state)")
    parser.add_argument("--lon", "--longitude", type=float, dest="longitude", help="Longitude coordinate (requires --lat, --city, --state)")
    parser.add_argument("--city", type=str, help="City name (used with --lat/--lon)")
    parser.add_argument("--state", type=str, help="Two-letter state code (used with --lat/--lon)")
    parser.add_argument("--days", type=int, default=7, help="Number of days of data to generate (default: 7)")

    args = parser.parse_args()

    # Determine mode: name-based or coordinate-based
    if args.latitude is not None or args.longitude is not None:
        # Coordinate-based mode
        if args.latitude is None or args.longitude is None:
            logger.error("✗ Both --lat and --lon must be specified together")
            sys.exit(1)

        if not args.city or not args.state:
            logger.error("✗ --city and --state are required when using --lat/--lon")
            sys.exit(1)

        if len(args.state) != 2:
            logger.error("✗ State must be a two-letter code (e.g., 'CO' for Colorado)")
            sys.exit(1)

        add_city_to_graph_by_coordinates(
            args.city,
            args.state.upper(),
            args.latitude,
            args.longitude,
            args.days
        )
    else:
        # Name-based mode (geocoding)
        if not args.city_name:
            logger.error("✗ City name is required (or use --lat/--lon with --city/--state)")
            parser.print_help()
            sys.exit(1)

        add_city_to_graph_by_name(args.city_name, args.days)


if __name__ == "__main__":
    main()
