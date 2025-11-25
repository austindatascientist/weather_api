"""
Location resolver for converting city names to coordinates.

This module provides shared geocoding functionality used by multiple scripts:
- weather data ingestion (ingest.py)
- graph node creation (add_city_to_graph.py)

Why this is necessary:
    The weather.gov API requires latitude and longitude coordinates, NOT city names.
    Their endpoints are structured as:
        https://api.weather.gov/points/{latitude},{longitude}

    Therefore, we must convert human-readable location names (e.g., "Denver")
    into precise coordinates before calling weather.gov.

Geocoding Service:
    Uses OpenStreetMap's Nominatim service (via geopy library) to resolve
    location names to coordinates. This is a free, open-source geocoding service.

Example:
    "Denver" → (39.7392, -104.9903)
    "San Diego, CA" → (32.7157, -117.1611)
"""

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from .config import USER_AGENT, API_TIMEOUT, get_logger

logger = get_logger(__name__)


class LocationInfo:
    """Container for location information."""

    def __init__(self, latitude: float, longitude: float, display_name: str,
                 city: str = None, state: str = None):
        self.latitude = latitude
        self.longitude = longitude
        self.display_name = display_name
        self.city = city
        self.state = state

    def __repr__(self):
        return f"LocationInfo(city={self.city}, state={self.state}, lat={self.latitude}, lon={self.longitude})"


def geocode_location(location_name: str) -> tuple[float, float, str]:
    """
    Convert a location name to coordinates using OpenStreetMap Nominatim.

    Args:
        location_name: Human-readable location (e.g., "San Diego", "Denver, CO")

    Returns:
        tuple: (latitude, longitude, full_address)

    Raises:
        ValueError: If location cannot be found or geocoding fails

    Example:
        >>> lat, lon, address = geocode_location("Denver")
        >>> print(f"{lat}, {lon}")
        39.7392, -104.9903
    """
    geolocator = Nominatim(user_agent=USER_AGENT)
    try:
        location = geolocator.geocode(location_name, timeout=API_TIMEOUT)
        if location is None:
            raise ValueError(f"Could not find location: {location_name}")
        return location.latitude, location.longitude, location.address
    except GeocoderTimedOut as error:
        raise ValueError(f"Geocoding timed out for: {location_name}") from error
    except GeocoderServiceError as error:
        raise ValueError(f"Geocoding service error: {error}") from error


def resolve_location(location_name: str) -> LocationInfo:
    """
    Resolve a location name to detailed location information.

    This is a higher-level function that returns structured location data,
    including extracted city and state information when available.

    Args:
        location_name: Human-readable location (e.g., "Denver", "San Diego, CA")

    Returns:
        LocationInfo: Object containing latitude, longitude, display_name, city, state

    Raises:
        ValueError: If location cannot be found or geocoding fails

    Example:
        >>> info = resolve_location("Denver, CO")
        >>> print(f"{info.city}, {info.state}: {info.latitude}, {info.longitude}")
        Denver, CO: 39.7392, -104.9903
    """
    lat, lon, display_name = geocode_location(location_name)

    # Try to extract city and state from display name
    # Display name format varies, but often: "City, County, State, Country"
    city = None
    state = None

    # US state name to abbreviation mapping
    state_abbrevs = {
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
        "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC"
    }

    parts = [p.strip() for p in display_name.split(',')]
    if len(parts) >= 1:
        city = parts[0]

    # Look for state in any part of the display name
    for part in parts:
        part = part.strip()
        # Check if it's a 2-letter state code
        if len(part) == 2 and part.upper() in state_abbrevs.values():
            state = part.upper()
            break
        # Check if it's a full state name
        if part in state_abbrevs:
            state = state_abbrevs[part]
            break

    return LocationInfo(
        latitude=lat,
        longitude=lon,
        display_name=display_name,
        city=city,
        state=state
    )


def validate_coordinates(latitude: float, longitude: float) -> bool:
    """
    Validate that coordinates are within valid ranges.

    Args:
        latitude: Latitude value (-90 to 90)
        longitude: Longitude value (-180 to 180)

    Returns:
        bool: True if coordinates are valid

    Raises:
        ValueError: If coordinates are out of valid range
    """
    if not (-90 <= latitude <= 90):
        raise ValueError(f"Latitude must be between -90 and 90, got {latitude}")

    if not (-180 <= longitude <= 180):
        raise ValueError(f"Longitude must be between -180 and 180, got {longitude}")

    return True
