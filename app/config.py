# app/config.py
"""Centralized configuration for the weather API application."""

import os
import logging
from pathlib import Path

import psycopg

# ==============================================================
# LOGGING CONFIGURATION
# ==============================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
if LOG_LEVEL not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
    LOG_LEVEL = "INFO"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)

# ==============================================================
# ENVIRONMENT VALIDATION
# ==============================================================

def _validate_port(value: str, name: str) -> int:
    """Validate and convert a port number."""
    try:
        port = int(value)
        if not (1 <= port <= 65535):
            raise ValueError(f"{name} must be between 1 and 65535, got {port}")
        return port
    except ValueError as e:
        raise ValueError(f"Invalid {name}: {e}")

def _validate_positive_int(value: str, name: str) -> int:
    """Validate and convert a positive integer."""
    try:
        num = int(value)
        if num < 0:
            raise ValueError(f"{name} must be non-negative, got {num}")
        return num
    except ValueError as e:
        raise ValueError(f"Invalid {name}: {e}")

def _validate_float(value: str, name: str) -> float:
    """Validate and convert a float."""
    try:
        return float(value)
    except ValueError as e:
        raise ValueError(f"Invalid {name}: {e}")

# ==============================================================
# DATABASE CONFIGURATION
# ==============================================================

# PostgreSQL connection parameters
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = _validate_port(os.getenv("POSTGRES_PORT", "5432"), "POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# SQLAlchemy DATABASE_URL - constructed from components or use explicit URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Table name for weather data
WEATHER_TABLE = os.getenv("WEATHER_TABLE", "weather_data")

# ==============================================================
# LOCATION DEFAULTS
# ==============================================================

DEFAULT_LAT = _validate_float(os.getenv("DEFAULT_LAT", "34.729847"), "DEFAULT_LAT")
DEFAULT_LON = _validate_float(os.getenv("DEFAULT_LON", "-86.5859011"), "DEFAULT_LON")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "America/Chicago")

# ==============================================================
# BACKUP CONFIGURATION
# ==============================================================

BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "/backups")).expanduser()
BACKUP_RETENTION_DAYS = _validate_positive_int(os.getenv("BACKUP_RETENTION_DAYS", "14"), "BACKUP_RETENTION_DAYS")

# ==============================================================
# API CONFIGURATION
# ==============================================================

USER_AGENT = os.getenv("USER_AGENT", "weather_api (github.com/austindatascientist/weather_api)")
API_TIMEOUT = _validate_positive_int(os.getenv("API_TIMEOUT", "10"), "API_TIMEOUT")
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

# ==============================================================
# UTILITY FUNCTIONS
# ==============================================================

def get_postgres_connection(**overrides):
    """
    Get an authenticated PostgreSQL connection using psycopg.

    Args:
        **overrides: Keyword arguments to replace default connection values.
                     Options: host, port, dbname, user, password
                     Example: get_postgres_connection(dbname='test_db')

    Returns:
        psycopg.Connection: A PostgreSQL connection
    """
    params = {
        'host': POSTGRES_HOST,
        'port': POSTGRES_PORT,
        'dbname': POSTGRES_DB,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
    }
    params.update(overrides)
    return psycopg.connect(**params)
