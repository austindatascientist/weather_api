# app/backup_data.py
"""
Export weather data from PostgreSQL to Parquet backup files.

This module provides automated backup functionality for the weather_data table:
- Exports data to compressed Parquet format (efficient columnar storage)
- Generates SHA256 checksums for data integrity verification
- Enforces retention policy to automatically delete old backups

Backup Format:
    - File: postgres_weather_data_YYYYMMDD_HHMMSS.parquet
    - Compression: ZSTD (high compression ratio, fast decompression)
    - Checksum: .sha256 file alongside each backup

Usage:
    # Direct execution
    python -m app.backup_data

    # Programmatic use
    from app.backup_data import backup_postgres
    parquet_path, sha_path = backup_postgres()

Configuration (via environment variables):
    BACKUP_DIR: Directory for backup files (default: /backups)
    BACKUP_RETENTION_DAYS: Days to keep old backups (default: 14)
"""

import json
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg import sql
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (
    WEATHER_TABLE, BACKUP_DIR, BACKUP_RETENTION_DAYS, get_logger, get_postgres_connection
)

logger = get_logger(__name__)

# Columns to export (excluding auto-increment id) - must match WeatherData model
BACKUP_COLUMNS = ["date", "location_name", "latitude", "longitude", "high_temp_f", "low_temp_f"]


def _now_utc():
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc)


def _ensure_dir(p: Path):
    """Create directory if it doesn't exist (including parents)."""
    p.mkdir(parents=True, exist_ok=True)


def _write_sha256(path: Path):
    """
    Generate SHA256 checksum file for a backup.

    Creates a .sha256 file in BSD-style format compatible with sha256sum -c.

    Args:
        path: Path to the file to checksum

    Returns:
        Path: Path to the created checksum file
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        # Read in 1MB chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    sha_path = path.with_suffix(path.suffix + ".sha256")
    sha_path.write_text(h.hexdigest() + "  " + path.name + "\n", encoding="utf-8")
    return sha_path


def _apply_retention(folder: Path, pattern: str, keep_days: int):
    """
    Delete backup files older than retention period.

    Args:
        folder: Directory containing backups
        pattern: Glob pattern to match backup files
        keep_days: Number of days to retain backups (0 = keep forever)

    Returns:
        list: Paths of deleted files
    """
    if keep_days <= 0:
        return []
    cutoff = _now_utc() - timedelta(days=keep_days)
    deleted = []
    for fp in folder.glob(pattern):
        ts = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)
        if ts < cutoff:
            try:
                fp.unlink(missing_ok=True)
                # Also delete associated checksum file
                sha = fp.with_suffix(fp.suffix + ".sha256")
                if sha.exists():
                    sha.unlink(missing_ok=True)
                deleted.append(fp)
                logger.debug(f"Deleted old backup: {fp}")
            except OSError as e:
                logger.warning(f"Failed to delete backup file {fp}: {e}")
    return deleted


def backup_postgres():
    """
    Export weather_data table to a compressed Parquet file.

    Creates a timestamped backup file with ZSTD compression and generates
    a SHA256 checksum. Applies retention policy to delete old backups.

    Returns:
        tuple: (parquet_file_path, sha256_file_path)

    Example:
        >>> parquet_path, sha_path = backup_postgres()
        >>> print(parquet_path)
        /backups/postgres_weather_data_20251125_030000.parquet
    """
    _ensure_dir(BACKUP_DIR)

    ts = _now_utc().strftime("%Y%m%d_%H%M%S")
    out_path = BACKUP_DIR / f"postgres_{WEATHER_TABLE}_{ts}.parquet"

    logger.info(f"Starting PostgreSQL backup to {out_path}")

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            # Use sql.Identifier for safe table name handling
            col_list = sql.SQL(", ").join([sql.Identifier(c) for c in BACKUP_COLUMNS])
            query = sql.SQL("""
                SELECT {}
                FROM {}
                ORDER BY date ASC, location_name ASC
            """).format(col_list, sql.Identifier(WEATHER_TABLE))
            cur.execute(query)
            rows = cur.fetchall()

    # Build Arrow table -> Parquet
    if rows:
        data = {col: values for col, values in zip(BACKUP_COLUMNS, zip(*rows))}
        table_pa = pa.table(data)
    else:
        table_pa = pa.table({c: pa.array([], type=pa.null()) for c in BACKUP_COLUMNS})

    pq.write_table(table_pa, out_path, compression="zstd", use_dictionary=True)
    sha = _write_sha256(out_path)

    logger.info(f"Backup completed: {len(rows)} rows written")

    # Retention
    deleted = _apply_retention(BACKUP_DIR, "postgres_*.parquet", BACKUP_RETENTION_DAYS)
    if deleted:
        logger.info(f"Retention policy applied: {len(deleted)} old backups deleted")

    return str(out_path), str(sha)


if __name__ == "__main__":
    out_path, sha_path = backup_postgres()
    print(json.dumps({"file": out_path, "sha256": sha_path}, indent=2))
