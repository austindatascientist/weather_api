# app/main.py
from contextlib import asynccontextmanager
from typing import Generator
import uuid

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from pydantic import BaseModel, Field
from datetime import datetime, date as date_cls
from zoneinfo import ZoneInfo
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware

from .orm_model import Base, engine, SessionLocal, WeatherData, WeatherDataOut
from .config import DEFAULT_LAT, DEFAULT_LON, DEFAULT_TIMEZONE, get_logger
from .graph_nodes import create_location_node, create_weather_nodes_from_api, create_edges_for_city
from .location_resolver import resolve_location

logger = get_logger(__name__)


# ==============================================================
# REQUEST ID MIDDLEWARE
# ==============================================================

class RequestIDMiddleware(BaseHTTPMiddleware):
    """Add unique request ID to each request for tracing."""

    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"[{request_id}] {request.method} {request.url.path}")

        response = await call_next(request)

        response.headers["X-Request-ID"] = request_id
        logger.info(f"[{request_id}] Response: {response.status_code}")
        return response


# ==============================================================
# APPLICATION LIFECYCLE
# ==============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    # Startup
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Application started")

    yield

    # Shutdown
    logger.info("Application shutting down")


app = FastAPI(title="Weather API", version="1.0.0", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)


# ==============================================================
# DEPENDENCIES
# ==============================================================

def get_session() -> Generator[Session, None, None]:
    """Yield a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_system_timezone() -> str:
    """Get system timezone, defaulting to configured timezone."""
    # Try /etc/timezone (Debian/Ubuntu)
    tz_file = Path("/etc/timezone")
    if tz_file.exists():
        tz = tz_file.read_text().strip()
        if tz:
            return tz
    # Try /etc/localtime symlink (most Linux)
    localtime = Path("/etc/localtime")
    if localtime.is_symlink():
        target = str(localtime.resolve())
        if "/zoneinfo/" in target:
            return target.split("/zoneinfo/")[-1]
    return DEFAULT_TIMEZONE

def resolve_today() -> str:
    tz = get_system_timezone()
    return datetime.now(ZoneInfo(tz)).date().isoformat()

def resolve_coordinates(lat: float | None, lon: float | None) -> tuple[float, float]:
    """Resolve coordinates, using defaults if not provided."""
    if lat is not None and lon is not None:
        return (lat, lon)
    return (DEFAULT_LAT, DEFAULT_LON)

def get_weather_by_date_and_location(db: Session, date: date_cls | str, lat: float, lon: float) -> WeatherData | None:
    """Query weather data for a specific date and location coordinates."""
    # Convert string to date if needed
    if isinstance(date, str):
        date = date_cls.fromisoformat(date)
    return db.execute(
        select(WeatherData).where(
            WeatherData.date == date,
            WeatherData.latitude == lat,
            WeatherData.longitude == lon
        )
    ).scalar_one_or_none()

def format_weather_response(row: WeatherData) -> dict[str, str | float]:
    """Format a WeatherData row as an API response."""
    return {
        "date": row.date.isoformat(),
        "high_temp": row.high_temp_f,
        "low_temp": row.low_temp_f,
        "location_name": row.location_name,
        "latitude": row.latitude,
        "longitude": row.longitude,
        "units": "F",
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ready")
def ready(db: Session = Depends(get_session)):
    # Database connectivity check
    try:
        db.execute(select(WeatherData).limit(1))
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")

@app.get("/weather/today", response_model=WeatherDataOut)
def weather_today(
    lat: float | None = Query(None, ge=-90, le=90),
    lon: float | None = Query(None, ge=-180, le=180),
    db: Session = Depends(get_session),
):
    today = resolve_today()
    resolved_lat, resolved_lon = resolve_coordinates(lat, lon)
    row = get_weather_by_date_and_location(db, today, resolved_lat, resolved_lon)
    if not row:
        raise HTTPException(status_code=404, detail="No data for today at this location")
    return format_weather_response(row)

@app.get("/weather/{date}", response_model=WeatherDataOut)
def weather_by_date(
    date: str,
    lat: float | None = Query(None, ge=-90, le=90),
    lon: float | None = Query(None, ge=-180, le=180),
    db: Session = Depends(get_session),
):
    try:
        d = date_cls.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD")

    resolved_lat, resolved_lon = resolve_coordinates(lat, lon)
    row = get_weather_by_date_and_location(db, d, resolved_lat, resolved_lon)
    if not row:
        raise HTTPException(status_code=404, detail="No data for that date at this location")
    return format_weather_response(row)


# ==============================================================
# APACHE AGE GRAPH ENDPOINTS
# ==============================================================

class CreateCityGraphNodesRequest(BaseModel):
    """Request body for creating graph nodes for a city."""
    city_name: str = Field(..., min_length=1, max_length=100, description="Name of the city (e.g., 'Denver', 'San Diego, CA')")
    state: str | None = Field(None, min_length=2, max_length=2, description="Two-letter state code (optional, derived from geocoding)")
    latitude: float | None = Field(None, ge=-90, le=90, description="Latitude coordinate (optional, geocoded from city name)")
    longitude: float | None = Field(None, ge=-180, le=180, description="Longitude coordinate (optional, geocoded from city name)")
    days: int = Field(7, ge=1, le=30, description="Number of days of historical data (default: 7)")


class CreateCityGraphNodesResponse(BaseModel):
    """Response for creating graph nodes for a city."""
    city: str
    state: str
    latitude: float
    longitude: float
    distance_to_coast_km: float
    nodes_created: dict
    edges_created: dict


@app.post("/api/graph/cities/nodes", response_model=CreateCityGraphNodesResponse)
def create_city_graph_nodes(request: CreateCityGraphNodesRequest):
    """
    Create graph nodes and edges for a city.

    Provide the city name to generate 7 days of weather graph nodes.

    Example:
        POST /api/graph/cities/nodes
        {"city_name": "Denver"}
    """
    try:
        # If coordinates not provided, geocode the city name
        if request.latitude is None or request.longitude is None:
            logger.info(f"Geocoding {request.city_name}...")
            location = resolve_location(request.city_name)
            latitude = location.latitude
            longitude = location.longitude
            state = request.state or location.state or "US"
            city_name = location.city or request.city_name
            logger.info(f"Resolved to: {location.display_name}")
        else:
            latitude = request.latitude
            longitude = request.longitude
            state = request.state or "US"
            city_name = request.city_name

        # Ensure state is 2 characters
        if len(state) != 2:
            state = "US"

        # Create Location node
        location_info = create_location_node(
            city_name,
            state,
            latitude,
            longitude
        )

        # Create weather nodes
        nodes_created = create_weather_nodes_from_api(city_name, latitude, longitude)

        # Create edges
        edges_created = create_edges_for_city(city_name)

        return {
            "city": city_name,
            "state": state,
            "latitude": latitude,
            "longitude": longitude,
            "distance_to_coast_km": location_info["distance_to_coast_km"],
            "nodes_created": nodes_created,
            "edges_created": edges_created
        }
    except ValueError as e:
        logger.error(f"Geocoding error for {request.city_name}: {e}")
        raise HTTPException(status_code=400, detail=f"Could not find location: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating graph nodes for {request.city_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create graph nodes: {str(e)}")
