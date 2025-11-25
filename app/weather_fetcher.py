"""
Centralized weather.gov API client.

This module provides a single, reusable interface for all weather.gov API interactions.
All scripts that need weather data should import from here rather than making direct
API calls, ensuring consistency and reducing code duplication.

API Flow:
    1. Call /points/{lat},{lon} to get grid metadata (forecast URLs, grid coordinates)
    2. Use the returned URLs to fetch specific data:
       - forecast: 7-day daily forecast (high/low temps)
       - forecastGridData: Detailed hourly data (temp, humidity, wind, etc.)

Usage:
    from app.weather_fetcher import WeatherAPI

    api = WeatherAPI()

    # Get 7-day forecast
    forecast = api.get_forecast(lat, lon)

    # Get detailed grid data
    grid_data = api.get_grid_data(lat, lon)

    # Get point metadata
    metadata = api.get_point_metadata(lat, lon)

Note:
    weather.gov API only works for US locations.
"""

from datetime import date, datetime
from typing import Optional

import requests

from .config import USER_AGENT, API_TIMEOUT, get_logger

logger = get_logger(__name__)


class WeatherAPIError(Exception):
    """Base exception for weather API errors."""
    pass


class LocationNotFoundError(WeatherAPIError):
    """Raised when location is outside US coverage area."""
    pass


class WeatherAPI:
    """
    Client for the National Weather Service (weather.gov) API.

    Provides methods for fetching weather forecasts and grid data.
    Handles the two-step API flow (points -> forecast/grid data) internally.

    Attributes:
        base_url: Base URL for the weather.gov API
        headers: HTTP headers including User-Agent (required by weather.gov)
        timeout: Request timeout in seconds
    """

    BASE_URL = "https://api.weather.gov"

    def __init__(self, user_agent: str = None, timeout: int = None):
        """
        Initialize the weather API client.

        Args:
            user_agent: Custom User-Agent string (uses config default if not provided)
            timeout: Request timeout in seconds (uses config default if not provided)
        """
        self.headers = {"User-Agent": user_agent or USER_AGENT}
        self.timeout = timeout or API_TIMEOUT
        self._points_cache = {}

    def _get(self, url: str) -> dict:
        """
        Make a GET request to the API.

        Args:
            url: Full URL to request

        Returns:
            dict: JSON response data

        Raises:
            LocationNotFoundError: If location is outside US
            WeatherAPIError: For other API errors
        """
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                raise LocationNotFoundError(
                    "Location not found. weather.gov only covers US locations."
                ) from e
            raise WeatherAPIError(f"API request failed: {e}") from e
        except requests.RequestException as e:
            raise WeatherAPIError(f"Request failed: {e}") from e

    def get_point_metadata(self, lat: float, lon: float) -> dict:
        """
        Get metadata for a geographic point.

        This is the first step in the weather.gov API flow. Returns information
        about the forecast grid that covers the given coordinates.

        Args:
            lat: Latitude coordinate (must be within US)
            lon: Longitude coordinate (must be within US)

        Returns:
            dict: Point metadata including forecast URLs and grid info

        Raises:
            LocationNotFoundError: If coordinates are outside US coverage
        """
        cache_key = f"{lat:.4f},{lon:.4f}"

        if cache_key not in self._points_cache:
            url = f"{self.BASE_URL}/points/{lat:.4f},{lon:.4f}"
            data = self._get(url)
            self._points_cache[cache_key] = data["properties"]

        return self._points_cache[cache_key]

    def get_forecast(self, lat: float, lon: float) -> list[dict]:
        """
        Fetch 7-day daily forecast from weather.gov API.

        Returns daily high/low temperatures for the next 7 days.

        Args:
            lat: Latitude coordinate (must be within US)
            lon: Longitude coordinate (must be within US)

        Returns:
            list[dict]: List of daily forecasts with keys:
                - date: date object
                - high_temp: float (Fahrenheit)
                - low_temp: float (Fahrenheit)

        Raises:
            LocationNotFoundError: If coordinates are outside US coverage
            WeatherAPIError: For other API errors
        """
        # Get forecast URL from point metadata
        metadata = self.get_point_metadata(lat, lon)
        forecast_url = metadata["forecast"]

        # Fetch forecast data
        data = self._get(forecast_url)
        periods = data["properties"]["periods"]

        # Group periods by date (alternating day/night)
        daily_data = {}
        for period in periods:
            date_str = period["startTime"][:10]
            d = date.fromisoformat(date_str)

            temp = period["temperature"]
            is_daytime = period["isDaytime"]

            if d not in daily_data:
                daily_data[d] = {"high": None, "low": None}

            if is_daytime:
                daily_data[d]["high"] = temp
            else:
                daily_data[d]["low"] = temp

        # Build result list, filtering incomplete days
        result = []
        for d, temps in sorted(daily_data.items()):
            high = temps["high"]
            low = temps["low"]

            if high is None or low is None:
                continue

            result.append({
                "date": d,
                "high_temp": float(high),
                "low_temp": float(low)
            })

        return result

    def get_grid_data(self, lat: float, lon: float) -> dict:
        """
        Fetch detailed hourly grid data from weather.gov API.

        Returns raw grid data including temperature, humidity, wind, and more.
        Use parse_grid_values() to extract specific properties.

        Args:
            lat: Latitude coordinate (must be within US)
            lon: Longitude coordinate (must be within US)

        Returns:
            dict: Raw grid properties from weather.gov

        Raises:
            LocationNotFoundError: If coordinates are outside US coverage
            WeatherAPIError: For other API errors
        """
        metadata = self.get_point_metadata(lat, lon)
        griddata_url = metadata["forecastGridData"]

        data = self._get(griddata_url)
        return data["properties"]

    def get_forecast_hourly(self, lat: float, lon: float) -> list[dict]:
        """
        Fetch hourly forecast from weather.gov API.

        Args:
            lat: Latitude coordinate (must be within US)
            lon: Longitude coordinate (must be within US)

        Returns:
            list[dict]: Hourly forecast periods

        Raises:
            LocationNotFoundError: If coordinates are outside US coverage
            WeatherAPIError: For other API errors
        """
        metadata = self.get_point_metadata(lat, lon)
        hourly_url = metadata["forecastHourly"]

        data = self._get(hourly_url)
        return data["properties"]["periods"]


def parse_grid_values(grid_data: dict, property_name: str,
                      convert_celsius: bool = True) -> list[tuple[datetime, float]]:
    """
    Parse weather.gov grid data values into (timestamp, value) pairs.

    Grid data uses ISO 8601 duration format for time periods:
    e.g., "2025-12-02T06:00:00+00:00/PT1H" means 1 hour starting at that time

    Args:
        grid_data: Raw grid data from get_grid_data()
        property_name: Name of property to extract (e.g., 'temperature', 'relativeHumidity')
        convert_celsius: If True, convert temperature values from C to F

    Returns:
        list of (datetime, value) tuples
    """
    if property_name not in grid_data:
        return []

    prop = grid_data[property_name]
    values = prop.get("values", [])
    unit = prop.get("uom", "")

    result = []
    for item in values:
        valid_time = item.get("validTime", "")
        if "/" in valid_time:
            timestamp_str = valid_time.split("/")[0]
        else:
            timestamp_str = valid_time

        try:
            if "+" in timestamp_str:
                timestamp_str = timestamp_str.rsplit("+", 1)[0]
            elif timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1]
            timestamp = datetime.fromisoformat(timestamp_str)
        except ValueError:
            continue

        value = item.get("value")
        if value is None:
            continue

        # Convert Celsius to Fahrenheit if needed
        if convert_celsius and "degC" in unit:
            value = value * 9 / 5 + 32

        result.append((timestamp, round(value, 2)))

    return result


# Module-level convenience instance
_default_api: Optional[WeatherAPI] = None


def get_api() -> WeatherAPI:
    """Get or create the default WeatherAPI instance."""
    global _default_api
    if _default_api is None:
        _default_api = WeatherAPI()
    return _default_api


# Convenience functions using the default API instance
def fetch_forecast(lat: float, lon: float) -> list[dict]:
    """Fetch 7-day forecast using the default API instance."""
    return get_api().get_forecast(lat, lon)


def fetch_grid_data(lat: float, lon: float) -> dict:
    """Fetch grid data using the default API instance."""
    return get_api().get_grid_data(lat, lon)
