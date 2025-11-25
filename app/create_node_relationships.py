"""
Create Temperature, Humidity, and Precipitation nodes with relationships using Apache AGE.

This module demonstrates weather data relationships:
- Temperature nodes: Individual temperature readings
- Humidity nodes: Individual humidity readings
- Precipitation nodes: Individual precipitation/rainfall readings
- Edges showing relationships:
  - CONCURRENT_WITH: Readings taken at same time/location
  - HEAT_INDEX_FACTOR: High temp + high humidity = dangerous conditions
  - INVERSE_CORRELATION: High temp, low humidity (pleasant)
  - HIGH_CORRELATION: Both high or both low
  - SUPPRESSES_RAIN: High temp with low humidity reduces rain probability
  - COOLING_EFFECT: Precipitation cools temperature
  - INCREASES_HUMIDITY: Rain increases humidity levels

Node Schema:
  Temperature: {timestamp, value_f, location, time_of_day, heat_category}
  Humidity: {timestamp, value_percent, location, time_of_day, comfort_level}
  Precipitation: {timestamp, value_inches, location, time_of_day, intensity}

Usage:
    python -m app.create_node_relationships
"""

from datetime import datetime, timedelta
import random

from .config import get_logger
from .age_utils import (
    age_cursor, GRAPH_NAME,
    categorize_temperature, categorize_humidity, categorize_precipitation,
    DEFAULT_CITIES, TIME_OF_DAY_PROFILES, DEFAULT_HISTORICAL_DAYS,
    PRECIP_SIGNIFICANT_THRESHOLD, PRECIP_HEAVY_THRESHOLD, TEMP_COOL_THRESHOLD,
    TEMP_HEAT_INDEX_THRESHOLD, TEMP_HIGH_THRESHOLD, TEMP_COLD_THRESHOLD, TEMP_PLEASANT_THRESHOLD,
    HUMIDITY_HEAT_INDEX_THRESHOLD, HUMIDITY_HIGH_THRESHOLD, HUMIDITY_VERY_HIGH_THRESHOLD, HUMIDITY_PLEASANT_MAX,
    random_temp, random_humidity, random_precipitation,
    create_weather_node, parse_agtype_count,
    format_timestamp
)

logger = get_logger(__name__)


def create_temperature_nodes():
    """Create Temperature nodes with realistic data patterns."""
    logger.info("Creating Temperature nodes...")

    temp_count = 0
    base_date = datetime.now() - timedelta(days=DEFAULT_HISTORICAL_DAYS)

    with age_cursor() as (cur, conn):
        for day in range(DEFAULT_HISTORICAL_DAYS):
            for city in DEFAULT_CITIES:
                for profile in TIME_OF_DAY_PROFILES:
                    timestamp = base_date + timedelta(days=day, hours=int(profile["time"].split(':')[0]))
                    ts_str = format_timestamp(timestamp)

                    temp_low, temp_high = profile["temp_range"]
                    temp_f = random_temp(temp_low, temp_high)
                    heat_cat = categorize_temperature(temp_f)

                    create_weather_node(cur, "Temperature", ts_str, city, profile["category"],
                                      "value_f", temp_f, "heat_category", heat_cat)
                    temp_count += 1

        conn.commit()
        logger.info(f"Created {temp_count} Temperature nodes")


def create_humidity_nodes():
    """Create Humidity nodes with realistic patterns (inverse to temperature)."""
    logger.info("Creating Humidity nodes...")

    humidity_count = 0
    base_date = datetime.now() - timedelta(days=DEFAULT_HISTORICAL_DAYS)

    with age_cursor() as (cur, conn):
        for day in range(DEFAULT_HISTORICAL_DAYS):
            for city in DEFAULT_CITIES:
                for profile in TIME_OF_DAY_PROFILES:
                    timestamp = base_date + timedelta(days=day, hours=int(profile["time"].split(':')[0]))
                    ts_str = format_timestamp(timestamp)

                    hum_low, hum_high = profile["humidity_range"]
                    humidity_percent = random_humidity(hum_low, hum_high)
                    comfort = categorize_humidity(humidity_percent)

                    create_weather_node(cur, "Humidity", ts_str, city, profile["category"],
                                      "value_percent", humidity_percent, "comfort_level", comfort)
                    humidity_count += 1

        conn.commit()
        logger.info(f"Created {humidity_count} Humidity nodes")


def create_precipitation_nodes():
    """Create Precipitation nodes - rain more likely with lower temps and higher humidity."""
    logger.info("Creating Precipitation nodes...")

    precip_count = 0
    base_date = datetime.now() - timedelta(days=DEFAULT_HISTORICAL_DAYS)

    with age_cursor() as (cur, conn):
        for day in range(DEFAULT_HISTORICAL_DAYS):
            for city in DEFAULT_CITIES:
                for profile in TIME_OF_DAY_PROFILES:
                    timestamp = base_date + timedelta(days=day, hours=int(profile["time"].split(':')[0]))
                    ts_str = format_timestamp(timestamp)

                    # Determine if it rains at this time
                    if random.random() < profile["rain_probability"]:
                        precip_inches = random_precipitation()
                    else:
                        precip_inches = 0.0

                    intensity = categorize_precipitation(precip_inches)

                    create_weather_node(cur, "Precipitation", ts_str, city, profile["category"],
                                      "value_inches", precip_inches, "intensity", intensity)
                    precip_count += 1

        conn.commit()
        logger.info(f"Created {precip_count} Precipitation nodes")


def create_concurrent_edges():
    """Create CONCURRENT_WITH edges between readings taken at the same time/location."""
    logger.info("Creating CONCURRENT_WITH edges...")

    with age_cursor() as (cur, conn):
        # Temperature <-> Humidity
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature), (h:Humidity)
                WHERE t.timestamp = h.timestamp AND t.location = h.location
                CREATE (t)-[r1:CONCURRENT_WITH]->(h)
                CREATE (h)-[r2:CONCURRENT_WITH]->(t)
                RETURN count(r1) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  Temperature <-> Humidity: {count} edges")

        # Temperature <-> Precipitation
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature), (p:Precipitation)
                WHERE t.timestamp = p.timestamp AND t.location = p.location
                CREATE (t)-[r1:CONCURRENT_WITH]->(p)
                CREATE (p)-[r2:CONCURRENT_WITH]->(t)
                RETURN count(r1) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  Temperature <-> Precipitation: {count} edges")

        # Humidity <-> Precipitation
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (h:Humidity), (p:Precipitation)
                WHERE h.timestamp = p.timestamp AND h.location = p.location
                CREATE (h)-[r1:CONCURRENT_WITH]->(p)
                CREATE (p)-[r2:CONCURRENT_WITH]->(h)
                RETURN count(r1) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  Humidity <-> Precipitation: {count} edges")

        conn.commit()


def create_temp_humidity_relationships():
    """Create relationships between temperature and humidity."""
    logger.info("Creating Temperature-Humidity relationship edges...")

    with age_cursor() as (cur, conn):
        # HEAT_INDEX_FACTOR: High temp + high humidity = dangerous
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:CONCURRENT_WITH]->(h:Humidity)
                WHERE t.value_f > {TEMP_HEAT_INDEX_THRESHOLD} AND h.value_percent > {HUMIDITY_HEAT_INDEX_THRESHOLD}
                CREATE (t)-[r:HEAT_INDEX_FACTOR {{severity: 'high', warning: 'dangerous heat index'}}]->(h)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  HEAT_INDEX_FACTOR: {count} edges")

        # INVERSE_CORRELATION: High temp, low humidity (pleasant)
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:CONCURRENT_WITH]->(h:Humidity)
                WHERE t.value_f > {TEMP_PLEASANT_THRESHOLD} AND h.value_percent < {HUMIDITY_PLEASANT_MAX}
                CREATE (t)-[r:INVERSE_CORRELATION {{pattern: 'pleasant_conditions', comfort: 'high'}}]->(h)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  INVERSE_CORRELATION: {count} edges")

        # HIGH_CORRELATION: Both high (muggy) or both low (cold/humid)
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:CONCURRENT_WITH]->(h:Humidity)
                WHERE (t.value_f > {TEMP_HEAT_INDEX_THRESHOLD} AND h.value_percent > {HUMIDITY_HIGH_THRESHOLD}) OR (t.value_f < 55 AND h.value_percent > 75)
                CREATE (t)-[r:HIGH_CORRELATION {{
                    pattern: CASE WHEN t.value_f > {TEMP_HEAT_INDEX_THRESHOLD} THEN 'muggy' ELSE 'cold_humid' END
                }}]->(h)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  HIGH_CORRELATION: {count} edges")

        conn.commit()


def create_temp_precipitation_relationships():
    """Create relationships between temperature and precipitation."""
    logger.info("Creating Temperature-Precipitation relationship edges...")

    with age_cursor() as (cur, conn):
        # COOLING_EFFECT: Precipitation cools temperature
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (p:Precipitation)-[:CONCURRENT_WITH]->(t:Temperature)
                WHERE p.value_inches > {PRECIP_SIGNIFICANT_THRESHOLD} AND t.value_f < {TEMP_COOL_THRESHOLD}
                CREATE (p)-[r:COOLING_EFFECT {{
                    impact: 'moderate',
                    description: 'rain brings cooler temperatures'
                }}]->(t)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  COOLING_EFFECT: {count} edges")

        # SUPPRESSES_RAIN: High temp with low humidity reduces rain probability
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:CONCURRENT_WITH]->(p:Precipitation)
                WHERE t.value_f > {TEMP_HIGH_THRESHOLD} AND p.value_inches = 0
                CREATE (t)-[r:SUPPRESSES_RAIN {{
                    reason: 'high_heat_low_moisture',
                    probability: 'low'
                }}]->(p)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  SUPPRESSES_RAIN: {count} edges")

        # COLD_RAIN: Lower temps with significant rain
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:CONCURRENT_WITH]->(p:Precipitation)
                WHERE t.value_f < {TEMP_COLD_THRESHOLD} AND p.value_inches > {PRECIP_HEAVY_THRESHOLD}
                CREATE (t)-[r:COLD_RAIN {{
                    condition: 'uncomfortable',
                    description: 'cold temperatures with heavy rain'
                }}]->(p)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  COLD_RAIN: {count} edges")

        conn.commit()


def create_humidity_precipitation_relationships():
    """Create relationships between humidity and precipitation."""
    logger.info("Creating Humidity-Precipitation relationship edges...")

    with age_cursor() as (cur, conn):
        # INCREASES_HUMIDITY: Rain increases humidity
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (p:Precipitation)-[:CONCURRENT_WITH]->(h:Humidity)
                WHERE p.value_inches > {PRECIP_SIGNIFICANT_THRESHOLD} AND h.value_percent > {HUMIDITY_HIGH_THRESHOLD}
                CREATE (p)-[r:INCREASES_HUMIDITY {{
                    effect: 'significant',
                    description: 'rain increases moisture in air'
                }}]->(h)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  INCREASES_HUMIDITY: {count} edges")

        # HIGH_HUMIDITY_NO_RAIN: Saturated air but no rain yet
        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (h:Humidity)-[:CONCURRENT_WITH]->(p:Precipitation)
                WHERE h.value_percent > {HUMIDITY_VERY_HIGH_THRESHOLD} AND p.value_inches = 0
                CREATE (h)-[r:HIGH_HUMIDITY_NO_RAIN {{
                    condition: 'pre_rain',
                    description: 'very humid but not raining yet'
                }}]->(p)
                RETURN count(r) as edge_count
            $$) as (edge_count agtype)
        """
        cur.execute(cypher)
        count = parse_agtype_count(cur.fetchone())
        logger.info(f"  HIGH_HUMIDITY_NO_RAIN: {count} edges")

        conn.commit()


def query_interesting_patterns():
    """Query and display interesting weather patterns."""
    logger.info("\n" + "="*80)
    logger.info("WEATHER RELATIONSHIP ANALYSIS")
    logger.info("="*80)

    with age_cursor() as (cur, conn):
        # Count nodes
        for node_type in ["Temperature", "Humidity", "Precipitation"]:
            cypher = f"""
                SELECT * FROM cypher('{GRAPH_NAME}', $$
                    MATCH (n:{node_type})
                    RETURN count(n) as node_count
                $$) as (node_count agtype)
            """
            cur.execute(cypher)
            count = parse_agtype_count(cur.fetchone())
            logger.info(f"{node_type} nodes: {count}")

        logger.info("\n" + "-"*80)
        logger.info("RELATIONSHIP EDGES:")
        logger.info("-"*80)

        edge_types = [
            "HEAT_INDEX_FACTOR", "INVERSE_CORRELATION", "HIGH_CORRELATION",
            "COOLING_EFFECT", "SUPPRESSES_RAIN", "COLD_RAIN",
            "INCREASES_HUMIDITY", "HIGH_HUMIDITY_NO_RAIN"
        ]

        for edge_type in edge_types:
            cypher = f"""
                SELECT * FROM cypher('{GRAPH_NAME}', $$
                    MATCH ()-[r:{edge_type}]->()
                    RETURN count(r) as edge_count
                $$) as (edge_count agtype)
            """
            cur.execute(cypher)
            count = parse_agtype_count(cur.fetchone())
            logger.info(f"  {edge_type}: {count}")

        # Dangerous heat index conditions
        logger.info("\n" + "-"*80)
        logger.info("DANGEROUS HEAT INDEX CONDITIONS (High Temp + High Humidity):")
        logger.info("-"*80)

        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:HEAT_INDEX_FACTOR]->(h:Humidity)
                RETURN t.location as location, t.timestamp as time,
                       t.value_f as temp, h.value_percent as humidity
                ORDER BY t.value_f DESC, h.value_percent DESC
                LIMIT 5
            $$) as (location agtype, time agtype, temp agtype, humidity agtype)
        """
        cur.execute(cypher)
        results = cur.fetchall()
        if results:
            for row in results:
                location = str(row[0]).strip('"')
                time = str(row[1]).strip('"')
                temp = str(row[2])
                humidity = str(row[3])
                logger.info(f"  {location} at {time}: {temp}°F, {humidity}% humidity")
        else:
            logger.info("  No dangerous conditions found")

        # Cold rain events
        logger.info("\n" + "-"*80)
        logger.info("COLD RAIN EVENTS (Low Temp + Heavy Rain):")
        logger.info("-"*80)

        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (t:Temperature)-[:COLD_RAIN]->(p:Precipitation)
                RETURN t.location as location, t.timestamp as time,
                       t.value_f as temp, p.value_inches as rain
                ORDER BY p.value_inches DESC
                LIMIT 5
            $$) as (location agtype, time agtype, temp agtype, rain agtype)
        """
        cur.execute(cypher)
        results = cur.fetchall()
        if results:
            for row in results:
                location = str(row[0]).strip('"')
                time = str(row[1]).strip('"')
                temp = str(row[2])
                rain = str(row[3])
                logger.info(f"  {location} at {time}: {temp}°F, {rain}\" rain")
        else:
            logger.info("  No cold rain events found")

        # Cooling effect of rain
        logger.info("\n" + "-"*80)
        logger.info("COOLING EFFECT OF RAIN (Rain + Lower Temps):")
        logger.info("-"*80)

        cypher = f"""
            SELECT * FROM cypher('{GRAPH_NAME}', $$
                MATCH (p:Precipitation)-[:COOLING_EFFECT]->(t:Temperature)
                RETURN p.location as location, p.timestamp as time,
                       p.value_inches as rain, t.value_f as temp
                ORDER BY p.value_inches DESC
                LIMIT 5
            $$) as (location agtype, time agtype, rain agtype, temp agtype)
        """
        cur.execute(cypher)
        results = cur.fetchall()
        if results:
            for row in results:
                location = str(row[0]).strip('"')
                time = str(row[1]).strip('"')
                rain = str(row[2])
                temp = str(row[3])
                logger.info(f"  {location} at {time}: {rain}\" rain, {temp}°F")
        else:
            logger.info("  No cooling rain events found")

        logger.info("="*80 + "\n")


def main():
    """Main function to create weather nodes with relationships."""
    logger.info("Starting Weather Relationship Graph Creation...")
    logger.info("This will create Temperature, Humidity, and Precipitation nodes")
    logger.info("with edges showing interesting weather patterns.\n")

    # Create nodes
    create_temperature_nodes()
    create_humidity_nodes()
    create_precipitation_nodes()

    # Create basic concurrent edges
    create_concurrent_edges()

    # Create relationship edges
    create_temp_humidity_relationships()
    create_temp_precipitation_relationships()
    create_humidity_precipitation_relationships()

    # Display interesting patterns
    query_interesting_patterns()

    logger.info("Weather relationship graph creation complete!")


if __name__ == "__main__":
    main()
