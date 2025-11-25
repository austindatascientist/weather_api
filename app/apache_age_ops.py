"""
Graph operations using Apache AGE extension for PostgreSQL.

This module provides functionality to:
- Create Location and WeatherReading nodes
- Create relationships (edges) between nodes

Usage:
    python -m app.apache_age_ops    # Initialize graph with sample data
"""

from datetime import date, timedelta
from math import radians, sin, cos, sqrt, atan2

from .config import get_logger
from .age_utils import age_cursor, GRAPH_NAME

logger = get_logger(__name__)

# Distance threshold for NEAR relationships (kilometers)
NEAR_DISTANCE_THRESHOLD_KM = 300

# Default number of days for sample weather data
DEFAULT_HISTORICAL_DAYS = 7


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance between two coordinates using the Haversine formula.

    The Haversine formula determines the shortest distance over the earth's surface,
    giving an "as-the-crow-flies" distance between two points.

    Args:
        lat1, lon1: First coordinate pair (degrees)
        lat2, lon2: Second coordinate pair (degrees)

    Returns:
        float: Distance in kilometers
    """
    R = 6371  # Earth's mean radius in kilometers

    # Convert decimal degrees to radians for trigonometric functions
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula: a = sin²(Δlat/2) + cos(lat1) × cos(lat2) × sin²(Δlon/2)
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    # c = 2 × atan2(√a, √(1−a)) gives the angular distance in radians
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c


def calculate_distance_to_coast(lat: float, lon: float) -> float:
    """
    Calculate minimum distance from coordinates to nearest US coastline.

    Uses reference points along Atlantic, Gulf, and Pacific coasts.
    Works for any US city coordinates provided by user.
    """
    # Reference points along major US coastlines
    # These coordinates are sampled at regular intervals to approximate coastlines
    # More points = better accuracy but slower computation
    coastal_references = [
        # Atlantic Coast (north to south)
        (44.3106, -68.7781),  # Maine
        (42.3601, -71.0589),  # Boston
        (40.7128, -74.0060),  # New York
        (38.9072, -77.0369),  # DC area
        (36.8529, -75.9780),  # Virginia Beach
        (33.9191, -78.9487),  # Myrtle Beach
        (32.0809, -80.9009),  # Charleston SC coast
        (31.5383, -81.3912),  # Savannah coast
        (30.3322, -81.6557),  # Jacksonville coast
        (25.7617, -80.1918),  # Miami

        # Gulf Coast (east to west)
        (30.3960, -86.4735),  # Destin FL
        (30.6944, -88.0431),  # Mobile Bay
        (29.3013, -89.4250),  # New Orleans coast
        (29.7604, -95.3698),  # Houston/Galveston
        (27.8006, -97.3964),  # Corpus Christi

        # Pacific Coast (south to north)
        (32.7157, -117.1611), # San Diego
        (33.7701, -118.1937), # Los Angeles coast
        (37.7749, -122.4194), # San Francisco
        (45.5152, -122.6784), # Portland area
        (47.6062, -122.3321), # Seattle
    ]

    # Find minimum distance to any coastal reference point
    min_distance = float('inf')
    for coast_lat, coast_lon in coastal_references:
        distance = haversine_distance(lat, lon, coast_lat, coast_lon)
        min_distance = min(min_distance, distance)

    return round(min_distance, 1)


def setup_graph():
    """
    Initialize the weather graph with sample Location nodes and relationships.

    Creates a graph structure with:
    - Location nodes for 9 Southeast US cities
    - NEAR edges between cities within 300km of each other
    - WeatherReading nodes with 7 days of sample forecast data
    - HAS_WEATHER edges connecting locations to their weather readings
    - NEXT_DAY edges linking consecutive weather readings

    This function is idempotent - MERGE ensures nodes aren't duplicated.
    """
    logger.info("Setting up graph with sample data...")

    # Sample locations representing a mix of inland and coastal cities
    # Distance to coast is calculated dynamically using coastal reference points
    locations = [
        # Inland cities
        {"name": "Huntsville", "state": "AL", "lat": 34.729847, "lon": -86.5859011},
        {"name": "Nashville", "state": "TN", "lat": 36.1622767, "lon": -86.7742984},
        {"name": "Birmingham", "state": "AL", "lat": 33.5206824, "lon": -86.8024326},
        {"name": "Atlanta", "state": "GA", "lat": 33.7544657, "lon": -84.3898151},
        {"name": "Chattanooga", "state": "TN", "lat": 35.0457219, "lon": -85.3094883},
        {"name": "Memphis", "state": "TN", "lat": 35.1460249, "lon": -90.0517638},
        # Coastal cities
        {"name": "Mobile", "state": "AL", "lat": 30.6913462, "lon": -88.0437509},
        {"name": "Savannah", "state": "GA", "lat": 32.0790074, "lon": -81.0921335},
        {"name": "Charleston", "state": "SC", "lat": 32.7884363, "lon": -79.9399309},
    ]

    with age_cursor() as (cur, conn):
        # Create Location nodes
        logger.info("Creating Location nodes...")
        for loc in locations:
            # Calculate distance to coast dynamically for any coordinates
            distance_to_coast = calculate_distance_to_coast(loc["lat"], loc["lon"])

            cypher = f"""
                SELECT * FROM cypher('{GRAPH_NAME}', $$
                    MERGE (l:Location {{name: '{loc["name"]}', state: '{loc["state"]}'}})
                    SET l.latitude = {loc["lat"]},
                        l.longitude = {loc["lon"]},
                        l.distance_to_coast_km = {distance_to_coast}
                    RETURN l
                $$) as (l agtype)
            """
            cur.execute(cypher)
            logger.info(f"  {loc['name']}: {distance_to_coast}km from coast")

        conn.commit()
        logger.info(f"Created {len(locations)} Location nodes")

        # Create NEAR relationships between nearby cities (within 300km)
        logger.info("Creating NEAR relationships...")
        near_count = 0
        for i, loc1 in enumerate(locations):
            for loc2 in locations[i+1:]:
                distance = haversine_distance(
                    loc1["lat"], loc1["lon"],
                    loc2["lat"], loc2["lon"]
                )
                if distance <= NEAR_DISTANCE_THRESHOLD_KM:
                    cypher = f"""
                        SELECT * FROM cypher('{GRAPH_NAME}', $$
                            MATCH (a:Location {{name: '{loc1["name"]}'}}),
                                  (b:Location {{name: '{loc2["name"]}'}})
                            MERGE (a)-[r:NEAR {{distance_km: {distance:.1f}}}]->(b)
                            MERGE (b)-[r2:NEAR {{distance_km: {distance:.1f}}}]->(a)
                            RETURN r
                        $$) as (r agtype)
                    """
                    cur.execute(cypher)
                    near_count += 1

        conn.commit()
        logger.info(f"Created {near_count} NEAR relationships")

        # Create sample WeatherReading nodes and HAS_WEATHER edges
        logger.info("Creating WeatherReading nodes and HAS_WEATHER edges...")
        weather_count = 0
        base_date = date.today()

        for loc in locations:
            # Create weather data for each location
            for day_offset in range(DEFAULT_HISTORICAL_DAYS):
                d = base_date + timedelta(days=day_offset)
                # Generate sample temps based on latitude (cooler up north)
                base_high = 75 - (loc["lat"] - 33) * 2
                base_low = base_high - 15
                high_temp = base_high + (day_offset % 3) * 2
                low_temp = base_low + (day_offset % 3)

                cypher = f"""
                    SELECT * FROM cypher('{GRAPH_NAME}', $$
                        MATCH (l:Location {{name: '{loc["name"]}'}})
                        CREATE (w:WeatherReading {{
                            date: '{d.isoformat()}',
                            high_temp_f: {high_temp:.1f},
                            low_temp_f: {low_temp:.1f}
                        }})
                        CREATE (l)-[:HAS_WEATHER]->(w)
                        RETURN w
                    $$) as (w agtype)
                """
                cur.execute(cypher)
                weather_count += 1

        conn.commit()
        logger.info(f"Created {weather_count} WeatherReading nodes with HAS_WEATHER edges")

        # Create NEXT_DAY relationships between consecutive weather readings
        logger.info("Creating NEXT_DAY relationships...")
        for loc in locations:
            for day_offset in range(DEFAULT_HISTORICAL_DAYS - 1):
                d1 = (base_date + timedelta(days=day_offset)).isoformat()
                d2 = (base_date + timedelta(days=day_offset + 1)).isoformat()

                cypher = f"""
                    SELECT * FROM cypher('{GRAPH_NAME}', $$
                        MATCH (l:Location {{name: '{loc["name"]}'}})-[:HAS_WEATHER]->(w1:WeatherReading {{date: '{d1}'}}),
                              (l)-[:HAS_WEATHER]->(w2:WeatherReading {{date: '{d2}'}})
                        MERGE (w1)-[:NEXT_DAY]->(w2)
                        RETURN w1, w2
                    $$) as (w1 agtype, w2 agtype)
                """
                cur.execute(cypher)

        conn.commit()
        logger.info("Created NEXT_DAY relationships")

    logger.info("Graph setup complete!")


def main():
    setup_graph()


if __name__ == "__main__":
    main()
