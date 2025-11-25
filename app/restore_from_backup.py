#!/usr/bin/env python3
"""
Restore Postgres from latest backups in /backups.

- Postgres: postgres_weather_data_*.parquet  -> table weather_data

Usage:
    python -m app.restore_from_backup
"""

from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql

from .config import (
    WEATHER_TABLE, BACKUP_DIR, get_logger, get_postgres_connection
)

logger = get_logger(__name__)


def find_latest_backup(prefix: str, suffix: str) -> Path:
    """
    Find the most recently modified file in BACKUP_DIR matching prefix + ... + suffix.

    Args:
        prefix: File name prefix (e.g., "postgres_weather_data_")
        suffix: File name suffix (e.g., ".parquet")

    Returns:
        Path: Path to the most recent matching backup file

    Raises:
        RuntimeError: If no matching backup files exist
    """
    # Filter files matching pattern and sort by modification time (oldest first)
    candidates = sorted(
        [p for p in BACKUP_DIR.iterdir()
         if p.is_file() and p.name.startswith(prefix) and p.name.endswith(suffix)],
        key=lambda p: p.stat().st_mtime,
    )
    if not candidates:
        raise RuntimeError(
            f"No backups matching {prefix}*{suffix} found in {BACKUP_DIR}"
        )
    # Return last element (most recent by modification time)
    return candidates[-1]


# -----------------------
# Postgres restore
# -----------------------

def restore_postgres() -> None:
    """
    Restore the weather_data table from the most recent Parquet backup.

    Process:
    1. Find the latest backup file by modification time
    2. Read the Parquet file into a DataFrame
    3. TRUNCATE the existing table (fast delete, resets auto-increment)
    4. Bulk load data using PostgreSQL COPY (fastest insert method)

    Raises:
        RuntimeError: If no backup files exist or database/file errors occur
    """
    parquet_path = find_latest_backup(
        prefix=f"postgres_{WEATHER_TABLE}_", suffix=".parquet"
    )
    logger.info(f"Restoring from: {parquet_path}")

    logger.info("Connecting to Postgres...")
    with get_postgres_connection() as conn:
        try:
            logger.info("Reading backup...")
            df = pd.read_parquet(parquet_path)

            # TRUNCATE is faster than DELETE and resets sequences
            logger.info(f"Truncating table {WEATHER_TABLE}...")
            with conn.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(WEATHER_TABLE)))

            # Build COPY statement with column names for bulk insert
            logger.info(f"Loading {len(df)} rows...")
            col_identifiers = sql.SQL(", ").join([sql.Identifier(c) for c in df.columns])
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(WEATHER_TABLE),
                col_identifiers
            )

            # Use COPY protocol for high-performance bulk loading
            with conn.cursor() as cur:
                with cur.copy(copy_sql.as_string(conn)) as copy:
                    # itertuples() is faster than iterrows() for large DataFrames
                    for row in df.itertuples(index=False):
                        copy.write_row(row)

            conn.commit()
            logger.info(f"Restored {len(df)} rows to {WEATHER_TABLE}")

        except psycopg.Error as error:
            conn.rollback()
            logger.error(f"Database error restoring Postgres: {error}")
            raise RuntimeError(f"Database restore failed: {error}") from error
        except IOError as error:
            conn.rollback()
            logger.error(f"File error restoring Postgres: {error}")
            raise RuntimeError(f"File read failed: {error}") from error


# -----------------------
# CLI
# -----------------------

def main() -> None:
    restore_postgres()


if __name__ == "__main__":
    main()
