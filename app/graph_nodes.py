"""
Create graph nodes and edges for individual cities.

This module provides functions to create Temperature, Humidity, and Precipitation
nodes with relationships for a specific city on-demand via API.
"""

import random
from datetime import datetime, timedelta

from .config import get_logger
from .age_utils import (
    age_cursor, GRAPH_NAME,
    categorize_temperature, categorize_humidity, categorize_precipitation,
    TIME_OF_DAY_PROFILES, DEFAULT_HISTORICAL_DAYS,
    PRECIP_SIGNIFICANT_THRESHOLD, TEMP_COOL_THRESHOLD, PRECIP_MAX,
    random_temp, random_humidity, random_precipitation,
    create_weather_node, create_concurrent_edges_between, parse_agtype_count,
    format_timestamp
)
from .apache_age_ops import calculate_distance_to_coast

logger = get_logger(__name__)


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


def create_weather_nodes_for_city(city_name: str, days: int = DEFAULT_HISTORICAL_DAYS):
    """
    Create Temperature, Humidity, and Precipitation nodes for a specific city.

    Args:
        city_name: Name of the city
        days: Number of days of historical data to generate (default: 7)

    Returns:
        dict with counts of nodes created
    """
    logger.info(f"Creating weather nodes for {city_name}...")

    temp_count = 0
    humidity_count = 0
    precip_count = 0
    base_date = datetime.now() - timedelta(days=days)

    with age_cursor() as (cur, conn):
        for day in range(days):
            for profile in TIME_OF_DAY_PROFILES:
                timestamp = base_date + timedelta(days=day, hours=int(profile["time"].split(':')[0]))
                ts_str = format_timestamp(timestamp)

                temp_low, temp_high = profile["temp_range"]
                hum_low, hum_high = profile["humidity_range"]
                rain_prob = profile["rain_probability"]
                time_category = profile["category"]

                # Temperature
                temp_f = random_temp(temp_low, temp_high)
                heat_cat = categorize_temperature(temp_f)
                create_weather_node(cur, "Temperature", ts_str, city_name, time_category,
                                  "value_f", temp_f, "heat_category", heat_cat)
                temp_count += 1

                # Humidity
                humidity_percent = random_humidity(hum_low, hum_high)
                comfort = categorize_humidity(humidity_percent)
                create_weather_node(cur, "Humidity", ts_str, city_name, time_category,
                                  "value_percent", humidity_percent, "comfort_level", comfort)
                humidity_count += 1

                # Precipitation
                if random.random() < rain_prob:
                    precip_inches = random_precipitation(PRECIP_MAX)
                else:
                    precip_inches = 0.0

                intensity = categorize_precipitation(precip_inches)
                create_weather_node(cur, "Precipitation", ts_str, city_name, time_category,
                                  "value_inches", precip_inches, "intensity", intensity)
                precip_count += 1

        conn.commit()

    logger.info(f"Created {temp_count} Temperature, {humidity_count} Humidity, {precip_count} Precipitation nodes for {city_name}")

    return {
        "temperature_nodes": temp_count,
        "humidity_nodes": humidity_count,
        "precipitation_nodes": precip_count
    }


def create_edges_for_city(city_name: str):
    """Create relationship edges between nodes for a specific city."""
    logger.info(f"Creating edges for {city_name}...")

    edge_counts = {}

    with age_cursor() as (cur, conn):
        # CONCURRENT_WITH edges (Temperature <-> Humidity <-> Precipitation)
        edge_counts['concurrent_temp_humidity'] = create_concurrent_edges_between(
            cur, "Temperature", "Humidity", city_name
        )

        edge_counts['concurrent_temp_precipitation'] = create_concurrent_edges_between(
            cur, "Temperature", "Precipitation", city_name
        )

        edge_counts['concurrent_humidity_precipitation'] = create_concurrent_edges_between(
            cur, "Humidity", "Precipitation", city_name
        )

        # COOLING_EFFECT: Precipitation cools temperature
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (p:Precipitation {{location: '{city_name}'}})-[:CONCURRENT_WITH]->(t:Temperature)
                WHERE p.value_inches > {PRECIP_SIGNIFICANT_THRESHOLD} AND t.value_f < {TEMP_COOL_THRESHOLD}
                CREATE (p)-[r:COOLING_EFFECT {{
                    impact: 'moderate',
                    description: 'rain brings cooler temperatures'
                }}]->(t)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        result = cur.fetchone()
        edge_counts['cooling_effect'] = parse_agtype_count(result) if result else 0

        conn.commit()

    logger.info(f"Created edges for {city_name}: {edge_counts}")

    return edge_counts
