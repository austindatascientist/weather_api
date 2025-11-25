"""
Shared utilities for Apache AGE graph operations.

This module provides:
- Database connection management
- AGE extension setup helpers
- Weather data categorization functions
"""

import random
from contextlib import contextmanager
from datetime import datetime

from .config import get_postgres_connection, get_logger

logger = get_logger(__name__)

GRAPH_NAME = "weather_graph"

# ==============================================================
# CONSTANTS
# ==============================================================

# Default cities for bulk operations
DEFAULT_CITIES = [
    "Huntsville", "Nashville", "Birmingham", "Atlanta",
    "Chattanooga", "Memphis", "Mobile", "Savannah", "Charleston"
]

# Time of day profiles with temperature, humidity ranges, and rain probability
TIME_OF_DAY_PROFILES = [
    {
        "time": "06:00",
        "category": "morning",
        "temp_range": (55, 65),
        "humidity_range": (70, 85),
        "rain_probability": 0.2
    },
    {
        "time": "12:00",
        "category": "midday",
        "temp_range": (75, 85),
        "humidity_range": (45, 60),
        "rain_probability": 0.1
    },
    {
        "time": "18:00",
        "category": "evening",
        "temp_range": (70, 80),
        "humidity_range": (55, 70),
        "rain_probability": 0.3
    },
    {
        "time": "00:00",
        "category": "night",
        "temp_range": (50, 60),
        "humidity_range": (75, 90),
        "rain_probability": 0.15
    }
]

# Temperature thresholds (Fahrenheit)
TEMP_COOL_THRESHOLD = 70
TEMP_HEAT_INDEX_THRESHOLD = 80
TEMP_HIGH_THRESHOLD = 85
TEMP_COLD_THRESHOLD = 60
TEMP_PLEASANT_THRESHOLD = 75

# Humidity thresholds (percent)
HUMIDITY_HEAT_INDEX_THRESHOLD = 60
HUMIDITY_HIGH_THRESHOLD = 70
HUMIDITY_VERY_HIGH_THRESHOLD = 85
HUMIDITY_PLEASANT_MAX = 55

# Precipitation constants (inches)
PRECIP_SIGNIFICANT_THRESHOLD = 0.1
PRECIP_HEAVY_THRESHOLD = 0.2
PRECIP_MIN = 0.01
PRECIP_MAX = 0.8

# Default number of historical days
DEFAULT_HISTORICAL_DAYS = 7


def get_age_connection():
    """
    Get a connection to PostgreSQL with AGE extension.

    Returns:
        psycopg.Connection: Database connection with AGE extension loaded
    """
    return get_postgres_connection()


@contextmanager
def age_cursor(conn=None):
    """
    Context manager for AGE operations with automatic setup.

    Handles:
    - Connection creation (if not provided)
    - AGE extension loading
    - Search path configuration
    - Cursor management

    Args:
        conn: Existing connection (optional). If None, creates new connection.

    Yields:
        tuple: (cursor, connection)

    Example:
        with age_cursor() as (cur, conn):
            cur.execute("SELECT * FROM cypher('weather_graph', $$...$$)")
            conn.commit()
    """
    close_conn = False
    if conn is None:
        conn = get_age_connection()
        close_conn = True

    try:
        with conn.cursor() as cur:
            # Load AGE extension and set search path
            cur.execute("LOAD 'age';")
            cur.execute("SET search_path = ag_catalog, \"$user\", public;")
            yield cur, conn
    finally:
        if close_conn:
            conn.close()


def setup_age_connection(conn):
    """
    Configure a connection for AGE operations.

    Loads AGE extension and sets search path.

    Args:
        conn: PostgreSQL connection
    """
    with conn.cursor() as cur:
        cur.execute("LOAD 'age';")
        cur.execute("SET search_path = ag_catalog, \"$user\", public;")


# ==============================================================
# WEATHER DATA CATEGORIZATION
# ==============================================================

def categorize_temperature(temp_f: float) -> str:
    """
    Categorize temperature into human-readable ranges.

    Args:
        temp_f: Temperature in Fahrenheit

    Returns:
        str: Category (freezing, cold, mild, warm, hot)
    """
    if temp_f < 32:
        return "freezing"
    elif temp_f < 50:
        return "cold"
    elif temp_f < 70:
        return "mild"
    elif temp_f < 85:
        return "warm"
    else:
        return "hot"


def categorize_humidity(humidity: float) -> str:
    """
    Categorize humidity comfort level.

    Args:
        humidity: Humidity percentage (0-100)

    Returns:
        str: Comfort level (dry, comfortable, humid, very_humid)
    """
    if humidity < 30:
        return "dry"
    elif humidity < 50:
        return "comfortable"
    elif humidity < 70:
        return "humid"
    else:
        return "very_humid"


def categorize_precipitation(precip_inches: float) -> str:
    """
    Categorize precipitation intensity.

    Args:
        precip_inches: Precipitation amount in inches

    Returns:
        str: Intensity (none, light, moderate, heavy)
    """
    if precip_inches == 0:
        return "none"
    elif precip_inches < 0.1:
        return "light"
    elif precip_inches < 0.3:
        return "moderate"
    else:
        return "heavy"


# ==============================================================
# UTILITY FUNCTIONS
# ==============================================================

def parse_agtype_count(result) -> int:
    """
    Parse AGE agtype count result to int.

    Args:
        result: Query result containing agtype count

    Returns:
        int: Parsed count value
    """
    if result is None:
        return 0
    return int(str(result[0]))


def format_timestamp(dt: datetime) -> str:
    """
    Format datetime as string for graph storage.

    Args:
        dt: datetime object

    Returns:
        str: Formatted timestamp (YYYY-MM-DD HH:MM:SS)
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def random_temp(low: float, high: float) -> float:
    """Generate random temperature within range."""
    return round(random.uniform(low, high), 1)


def random_humidity(low: float, high: float) -> float:
    """Generate random humidity within range."""
    return round(random.uniform(low, high), 1)


def random_precipitation(max_val: float = PRECIP_MAX) -> float:
    """Generate random precipitation amount."""
    return round(random.uniform(PRECIP_MIN, max_val), 2)


def create_weather_node(cur, node_type: str, timestamp: str, location: str,
                       time_category: str, value_field: str, value: float,
                       category_field: str, category: str):
    """
    Create a weather node (Temperature, Humidity, or Precipitation).

    Args:
        cur: Database cursor
        node_type: Node type (Temperature, Humidity, Precipitation)
        timestamp: Timestamp string
        location: Location name
        time_category: Time of day category
        value_field: Name of value field (e.g., 'value_f', 'value_percent')
        value: Numeric value
        category_field: Name of category field (e.g., 'heat_category', 'comfort_level')
        category: Category string
    """
    cypher = f"""
        SELECT * FROM cypher('{GRAPH_NAME}', $$
            CREATE (n:{node_type} {{
                timestamp: '{timestamp}',
                {value_field}: {value},
                location: '{location}',
                time_of_day: '{time_category}',
                {category_field}: '{category}'
            }})
            RETURN n
        $$) as (n agtype)
    """
    cur.execute(cypher)


def create_concurrent_edges_between(cur, node_type1: str, node_type2: str,
                                   location: str) -> int:
    """
    Create bidirectional CONCURRENT_WITH edges between two node types.

    Args:
        cur: Database cursor
        node_type1: First node type
        node_type2: Second node type
        location: Location name

    Returns:
        int: Number of edges created (one direction)
    """
    cypher = f"""
        SELECT * FROM cypher('{GRAPH_NAME}', $$
            MATCH (n1:{node_type1} {{location: '{location}'}}),
                  (n2:{node_type2} {{location: '{location}'}})
            WHERE n1.timestamp = n2.timestamp
            CREATE (n1)-[r1:CONCURRENT_WITH]->(n2)
            CREATE (n2)-[r2:CONCURRENT_WITH]->(n1)
            RETURN count(r1) as edge_count
        $$) as (edge_count agtype)
    """
    cur.execute(cypher)
    result = cur.fetchone()
    return parse_agtype_count(result) if result else 0
