"""
Create Temperature and Humidity nodes with relationships using Apache AGE.

This module creates weather graph nodes for the default cities using real data
from the weather.gov API. It creates:
- Temperature nodes: Individual temperature readings
- Humidity nodes: Individual humidity readings
- Location nodes: City information with coastal distance
- Edges showing relationships:
  - CONCURRENT_WITH: Readings taken at same time/location

Node Schema:
  Location: {name, state, latitude, longitude, distance_to_coast_km}
  Temperature: {timestamp, value_f, location, time_of_day, heat_category}
  Humidity: {timestamp, value_percent, location, time_of_day, comfort_level}

Usage:
    python -m app.create_node_relationships
"""

import json
import re
from datetime import datetime

import requests

from .config import get_logger
from .graph_nodes import create_city_graph

logger = get_logger(__name__)

# State name to abbreviation mapping
STATE_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"
}

# Hardcoded fallback list (2025 US Census estimates)
DEFAULT_CITIES_2025 = [
    "New York City, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "Fort Worth, TX"
]


def fetch_top_10_cities() -> list[str]:
    """
    Fetch top 10 most populous US cities from worldpopulationreview.com.

    Falls back to hardcoded 2025 list if fetch fails.

    Returns:
        list[str]: List of city names with state abbreviations
    """
    current_year = datetime.now().year
    url = "https://worldpopulationreview.com/us-cities"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text

        # Find the JSON data embedded in the page (assigned to const data = "[...]")
        match = re.search(r'const\s+data\s*=\s*"(\[.*?\])"\s*;', html, re.DOTALL)
        if match:
            # The JSON is escaped, need to decode it
            json_str = match.group(1).encode().decode('unicode_escape')
            cities_data = json.loads(json_str)

            # Sort by population (pop2025 or population field) and take top 10
            cities_data.sort(key=lambda x: x.get("pop2025", x.get("population", 0)), reverse=True)

            cities = []
            for city in cities_data[:10]:
                name = city.get("city", "")
                state = city.get("state", "")
                # Convert state name to abbreviation
                state_abbrev = STATE_ABBREV.get(state, state)
                cities.append(f"{name}, {state_abbrev}")

            logger.info("Fetched top 10 US cities by population from worldpopulationreview.com")
            return cities

        raise ValueError("Could not parse city data from worldpopulationreview.com")

    except Exception as e:
        logger.warning(f"Could not fetch city data: {e}")
        logger.warning(f"Defaulting to hard-coded {current_year} list of 10 most populous US cities")
        return DEFAULT_CITIES_2025


def main():
    """Main function to create weather nodes with relationships for top 10 US cities."""
    logger.info("Starting Weather Relationship Graph Creation...")

    # Fetch top 10 cities (with fallback to hardcoded list)
    cities = fetch_top_10_cities()

    logger.info(f"Creating graph nodes for {len(cities)} cities...")
    logger.info(f"Cities: {', '.join(cities)}\n")

    success_count = 0
    for city in cities:
        try:
            logger.info(f"Processing {city}...")
            result = create_city_graph(city)
            logger.info(f"  Created {result['nodes_created']['temperature_nodes']} temperature nodes")
            logger.info(f"  Created {result['nodes_created']['humidity_nodes']} humidity nodes")
            success_count += 1
        except Exception as e:
            logger.warning(f"Could not create graph for {city}: {e}")

    logger.info(f"\nWeather relationship graph creation complete!")
    logger.info(f"Successfully processed {success_count}/{len(cities)} cities")


if __name__ == "__main__":
    main()
