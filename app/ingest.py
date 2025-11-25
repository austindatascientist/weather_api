"""
Weather data ingestion from weather.gov API.

This module fetches 7-day weather forecasts from the National Weather Service (NWS)
API and stores them in the PostgreSQL database. It handles:
- Location geocoding (city name → coordinates)
- Weather.gov API calls (coordinates → forecast data)
- Database upsert (insert or update existing records)

API Flow:
    1. User provides city name (e.g., "Denver")
    2. Geocode city to coordinates via OpenStreetMap Nominatim
    3. Call weather.gov /points/{lat},{lon} to get forecast grid
    4. Fetch 7-day forecast from the returned forecast URL
    5. Parse daily high/low temperatures
    6. Upsert each day's data into weather_data table

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

import requests

from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from .orm_model import SessionLocal, WeatherData
from .config import USER_AGENT, API_TIMEOUT, get_logger
from .location_resolver import geocode_location

logger = get_logger(__name__)


def fetch_weather_gov_forecast(lat: float, lon: float) -> list[dict]:
    """
    Fetch forecast data from weather.gov API.

    The weather.gov API uses a two-step process:
    1. Call /points/{lat},{lon} to get the forecast grid location
    2. Call the returned forecast URL to get actual forecast data

    Args:
        lat: Latitude coordinate (must be within US)
        lon: Longitude coordinate (must be within US)

    Returns:
        list[dict]: List of daily forecasts with keys: date, high_temp, low_temp

    Raises:
        requests.RequestException: If API calls fail
    """
    headers = {"User-Agent": USER_AGENT}

    # Step 1: Get grid point info - weather.gov requires this intermediate step
    # to determine which forecast office and grid coordinates serve this location
    points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    resp = requests.get(points_url, headers=headers, timeout=API_TIMEOUT)
    resp.raise_for_status()

    # Extract the forecast URL from the points response
    # This URL points to the specific forecast office's data
    forecast_url = resp.json()["properties"]["forecast"]

    # Step 2: Fetch the actual forecast data
    resp = requests.get(forecast_url, headers=headers, timeout=API_TIMEOUT)
    resp.raise_for_status()

    # The forecast returns alternating day/night periods
    # Each period has isDaytime=True (high temp) or False (low temp)
    periods = resp.json()["properties"]["periods"]

    # Group periods by date to combine day/night temps into daily records
    daily_data = {}
    for period in periods:
        # Extract date from ISO timestamp (e.g., "2025-11-18T06:00:00-06:00" → "2025-11-18")
        date_str = period["startTime"][:10]
        d = date.fromisoformat(date_str)

        temp = period["temperature"]
        is_daytime = period["isDaytime"]

        if d not in daily_data:
            daily_data[d] = {"high": None, "low": None}

        # Daytime periods contain high temps, nighttime contains lows
        if is_daytime:
            daily_data[d]["high"] = temp
        else:
            daily_data[d]["low"] = temp

    # Convert grouped data to list, filtering incomplete days
    result = []
    for d, temps in sorted(daily_data.items()):
        high = temps["high"]
        low = temps["low"]

        # Skip days missing either temperature (typically first/last day of forecast)
        if high is None or low is None:
            continue

        result.append({
            "date": d,
            "high_temp": float(high),
            "low_temp": float(low)
        })

    return result




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
        forecast_data = fetch_weather_gov_forecast(lat, lon)
    except requests.RequestException as e:
        error_msg = str(e)
        if "404" in error_msg or "Not Found" in error_msg:
            logger.error("Weather.gov API error: Location may be outside the United States.")
            logger.error("The weather.gov API only provides forecasts for US locations.")
        else:
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
