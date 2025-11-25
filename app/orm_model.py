# app/orm_model.py
"""Database configuration, ORM models, and API schemas."""

from sqlalchemy import create_engine, Column, Integer, Date, Float, String, UniqueConstraint, Index
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from pydantic import BaseModel, Field

from .config import DATABASE_URL


# ==============================================================
# DATABASE CONFIGURATION
# ==============================================================

class Base(DeclarativeBase):
    pass

# Connection pool configuration
engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_size=5,           # Number of persistent connections
    max_overflow=10,       # Additional connections when pool is exhausted
    pool_timeout=30,       # Seconds to wait for available connection
    pool_recycle=1800,     # Recycle connections after 30 minutes
    pool_pre_ping=True,    # Verify connection health before use
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# ==============================================================
# ORM MODELS
# ==============================================================

class WeatherData(Base):
    """Weather data model for storing forecasts by date and location."""

    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)  # e.g., 2025-10-08
    high_temp_f = Column(Float, nullable=False)
    low_temp_f = Column(Float, nullable=False)
    location_name = Column(String(128), nullable=False)  # e.g., "Huntsville, AL"
    latitude = Column(Float, nullable=False)  # e.g., 34.7298
    longitude = Column(Float, nullable=False)  # e.g., -86.5859

    # Enforce uniqueness per (date, lat, lon) to allow multiple cities in DB
    __table_args__ = (
        UniqueConstraint("date", "latitude", "longitude", name="uq_weather_data_date_coords"),
        Index("ix_weather_data_date", "date"),
        Index("ix_weather_data_location_name", "location_name"),
        Index("ix_weather_data_coords", "latitude", "longitude"),
    )

    def __repr__(self):
        return (
            f"<WeatherData(date={self.date}, "
            f"high_temp_f={self.high_temp_f}, "
            f"low_temp_f={self.low_temp_f}, "
            f"location_name='{self.location_name}', "
            f"latitude={self.latitude}, longitude={self.longitude})>"
        )


# ==============================================================
# API SCHEMAS (Pydantic)
# ==============================================================

class WeatherDataOut(BaseModel):
    """Response schema for weather data."""
    date: str = Field(description="Date in ISO format")
    high_temp: float = Field(alias="high_temp_f", description="High temperature")
    low_temp: float = Field(alias="low_temp_f", description="Low temperature")
    units: str = "F"
    location_name: str = Field(description="Human-readable location name")
    latitude: float = Field(description="Latitude coordinate")
    longitude: float = Field(description="Longitude coordinate")

    class Config:
        populate_by_name = True
