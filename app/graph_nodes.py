"""
Create graph nodes and edges for individual cities using real weather.gov data.

This module provides functions to create Temperature and Humidity nodes with
relationships for a specific city using data from the National Weather Service
(NWS) API.

Can be called directly as a script:
    python -m app.graph_nodes "San Diego"
    python -m app.graph_nodes "Denver, CO"

Or imported and used programmatically:
    from app.graph_nodes import create_city_graph
    create_city_graph("San Diego")
"""

import argparse
import sys
from datetime import datetime

from .config import get_logger
from .location_resolver import resolve_location
from .age_utils import (
    age_cursor, GRAPH_NAME,
    categorize_temperature, categorize_humidity,
    create_weather_node, create_concurrent_edges_between,
    format_timestamp
)
from .apache_age_ops import calculate_distance_to_coast
from .weather_fetcher import fetch_grid_data, parse_grid_values, WeatherAPIError

logger = get_logger(__name__)


def get_time_category(hour: int) -> str:
    """Categorize hour into time of day."""
    if 5 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "midday"
    elif 17 <= hour < 21:
        return "evening"
    else:
        return "night"


def create_location_node(city_name: str, state: str, lat: float, lon: float):
    """Create a Location node for the city."""
    distance_to_coast = calculate_distance_to_coast(lat, lon)

    with age_cursor() as (cur, conn):
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MERGE (l:Location {{name: '{city_name}', state: '{state}'}})
                SET l.latitude = {lat},
                    l.longitude = {lon},
                    l.distance_to_coast_km = {distance_to_coast}
                RETURN l
            $$) as (l agtype)
        """
        cur.execute(cypher)
        conn.commit()

    logger.info(f"Created Location node for {city_name}: {distance_to_coast}km from coast")
    return {"city": city_name, "distance_to_coast_km": distance_to_coast}


def create_weather_nodes_from_api(city_name: str, lat: float, lon: float) -> dict:
    """
    Create Temperature and Humidity nodes from weather.gov API data.

    Args:
        city_name: Name of the city
        lat: Latitude coordinate
        lon: Longitude coordinate

    Returns:
        dict with counts of nodes created
    """
    logger.info(f"Fetching weather data from weather.gov for {city_name}...")

    try:
        grid_data = fetch_grid_data(lat, lon)
    except WeatherAPIError as e:
        logger.error(f"Failed to fetch weather data: {e}")
        raise

    # Parse weather properties
    temperatures = parse_grid_values(grid_data, "temperature")
    humidities = parse_grid_values(grid_data, "relativeHumidity")

    logger.info(f"Retrieved {len(temperatures)} temp, {len(humidities)} humidity readings")

    # Create a lookup for humidity by timestamp
    humidity_lookup = {ts: val for ts, val in humidities}

    temp_count = 0
    humidity_count = 0

    with age_cursor() as (cur, conn):
        for timestamp, temp_f in temperatures:
            ts_str = format_timestamp(timestamp)
            time_category = get_time_category(timestamp.hour)

            # Create Temperature node
            heat_cat = categorize_temperature(temp_f)
            create_weather_node(cur, "Temperature", ts_str, city_name, time_category,
                              "value_f", temp_f, "heat_category", heat_cat)
            temp_count += 1

            # Create Humidity node if we have data for this timestamp
            humidity = humidity_lookup.get(timestamp)
            if humidity is not None:
                comfort = categorize_humidity(humidity)
                create_weather_node(cur, "Humidity", ts_str, city_name, time_category,
                                  "value_percent", humidity, "comfort_level", comfort)
                humidity_count += 1

        conn.commit()

    logger.info(f"Created {temp_count} Temperature, {humidity_count} Humidity nodes for {city_name}")

    return {
        "temperature_nodes": temp_count,
        "humidity_nodes": humidity_count
    }


def create_edges_for_city(city_name: str):
    """Create relationship edges between nodes for a specific city."""
    logger.info(f"Creating edges for {city_name}...")

    edge_counts = {}

    with age_cursor() as (cur, conn):
        # CONCURRENT_WITH edges (Temperature <-> Humidity)
        edge_counts['concurrent_temp_humidity'] = create_concurrent_edges_between(
            cur, "Temperature", "Humidity", city_name
        )

        conn.commit()

    logger.info(f"Created edges for {city_name}: {edge_counts}")

    return edge_counts


def create_city_graph(location_name: str) -> dict:
    """
    Create all graph nodes and relationships for a city using real weather.gov data.

    This is the main entry point for creating graph data for a city.
    It handles geocoding, fetches real weather data from weather.gov,
    creates Location/Temperature/Humidity nodes, and establishes relationship edges.

    Args:
        location_name: City name (e.g., "San Diego", "Denver, CO")

    Returns:
        dict: Summary of created nodes and edges

    Example:
        >>> result = create_city_graph("San Diego")
        >>> print(result['nodes_created']['temperature_nodes'])
        156
    """
    # Resolve location to get coordinates and structured info
    try:
        location_info = resolve_location(location_name)
        city = location_info.city or location_name
        state = location_info.state or "US"
        lat = location_info.latitude
        lon = location_info.longitude
        logger.info(f"Resolved {location_name} -> {city}, {state} ({lat:.4f}, {lon:.4f})")
    except ValueError as e:
        logger.error(f"Could not resolve location '{location_name}': {e}")
        raise

    # Create Location node
    location_result = create_location_node(city, state, lat, lon)

    # Create weather nodes from real weather.gov API data
    nodes_created = create_weather_nodes_from_api(city, lat, lon)

    # Create relationship edges
    edges_created = create_edges_for_city(city)

    return {
        "city": city,
        "state": state,
        "latitude": lat,
        "longitude": lon,
        "distance_to_coast_km": location_result["distance_to_coast_km"],
        "nodes_created": nodes_created,
        "edges_created": edges_created
    }


def main():
    """CLI entry point for creating graph nodes for a city."""
    parser = argparse.ArgumentParser(
        description="Create graph nodes and relationships for a city using weather.gov data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.graph_nodes "San Diego"
  python -m app.graph_nodes "Denver, CO"
  python -m app.graph_nodes "Seattle"
        """
    )
    parser.add_argument("location", type=str, help="City name (e.g., 'San Diego', 'Denver, CO')")

    args = parser.parse_args()

    try:
        result = create_city_graph(args.location)
        logger.info(f"Successfully created graph for {result['city']}, {result['state']}")
        logger.info(f"  Distance to coast: {result['distance_to_coast_km']}km")
        logger.info(f"  Temperature nodes: {result['nodes_created']['temperature_nodes']}")
        logger.info(f"  Humidity nodes: {result['nodes_created']['humidity_nodes']}")
        logger.info(f"  Total edges: {sum(result['edges_created'].values())}")
    except ValueError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
