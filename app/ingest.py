"""
Weather data ingestion from weather.gov API.

This module fetches 7-day weather forecasts from the National Weather Service (NWS)
API and stores them in the PostgreSQL database. It handles:
- Location geocoding (city name → coordinates)
- Weather.gov API calls (via weather_fetcher module)
- Database upsert (insert or update existing records)

Usage:
    python -m app.ingest "San Diego"
    python -m app.ingest "New York City"
    python -m app.ingest "Huntsville, AL"

Note:
    weather.gov API only works for US locations.
"""
import argparse
import sys
from datetime import date

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from .orm_model import SessionLocal, WeatherData
from .config import get_logger
from .location_resolver import geocode_location
from .graph_nodes import create_city_graph
from .weather_fetcher import fetch_forecast, WeatherAPIError, LocationNotFoundError

logger = get_logger(__name__)




def upsert_weather(db: Session, d: date, hi: float, lo: float,
                   location_name: str, lat: float, lon: float) -> None:
    """
    Insert or update weather data for a specific date and location.

    Uses PostgreSQL's ON CONFLICT (upsert) to handle duplicate entries gracefully.
    If a record with the same (date, latitude, longitude) exists, it updates
    the temperatures; otherwise, it inserts a new record.

    Args:
        db: SQLAlchemy database session
        d: Forecast date
        hi: High temperature in Fahrenheit
        lo: Low temperature in Fahrenheit
        location_name: User-provided location string (e.g., "Denver, CO")
        lat: Latitude coordinate
        lon: Longitude coordinate
    """
    # Build upsert statement using PostgreSQL dialect
    stmt = (
        insert(WeatherData)
        .values(
            date=d,
            high_temp_f=hi,
            low_temp_f=lo,
            location_name=location_name,
            latitude=lat,
            longitude=lon
        )
        # ON CONFLICT: if (date, lat, lon) already exists, update temps instead of error
        .on_conflict_do_update(
            index_elements=["date", "latitude", "longitude"],
            set_={"high_temp_f": hi, "low_temp_f": lo, "location_name": location_name},
        )
    )
    db.execute(stmt)


def ingest_weather(location_name: str) -> None:
    """
    Fetch weather data from weather.gov and store in database.
    """
    # Convert location name to coordinates
    try:
        lat, lon, display_name = geocode_location(location_name)
        logger.info(f"Location: {display_name}")
        logger.info(f"Coordinates: {lat:.4f}, {lon:.4f}")
    except ValueError as e:
        logger.error(f"Geocoding error: {e}")
        sys.exit(1)

    # Fetch forecast from weather.gov
    try:
        logger.info("Fetching forecast from weather.gov...")
        forecast_data = fetch_forecast(lat, lon)
    except LocationNotFoundError:
        logger.error("Weather.gov API error: Location may be outside the United States.")
        logger.error("The weather.gov API only provides forecasts for US locations.")
        sys.exit(1)
    except WeatherAPIError as e:
        logger.error(f"Error fetching weather data: {e}")
        sys.exit(1)

    # Save all forecast data
    with SessionLocal() as db:
        for day in forecast_data:
            upsert_weather(
                db, day["date"], day["high_temp"], day["low_temp"],
                location_name, lat, lon
            )
        db.commit()

    logger.info(f"Saved {len(forecast_data)} days of weather data")

    # Create graph nodes and relationships
    logger.info("Creating graph nodes and relationships...")
    try:
        result = create_city_graph(location_name)
        logger.info(f"Graph nodes created for {result['city']}")
    except Exception as e:
        logger.warning(f"Could not create graph nodes: {e}")
        logger.warning("Weather data was saved, but graph nodes were not created.")


def main():
    p = argparse.ArgumentParser(
        description="Fetch 7-day weather forecast from weather.gov (US locations only)",
        epilog="""
Examples:
  python -m app.ingest "San Diego"
  python -m app.ingest "New York City"
  python -m app.ingest "Huntsville, AL"
        """
    )
    p.add_argument("location", type=str, help="Location name (e.g., 'San Diego', 'New York City')")
    args = p.parse_args()

    ingest_weather(args.location)


if __name__ == "__main__":
    main()
