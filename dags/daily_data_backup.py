"""
Airflow DAG for automated daily PostgreSQL backup.

This DAG runs the backup_postgres() function from app.backup_data to create
compressed Parquet snapshots of the weather_data table. Backups are stored
in /backups with automatic retention policy enforcement.

Schedule:
    Default: 3:00 AM daily (configurable via BACKUP_SCHEDULE_CRON env var)

Output:
    - Parquet file: /backups/postgres_weather_data_YYYYMMDD_HHMMSS.parquet
    - SHA256 checksum: /backups/postgres_weather_data_YYYYMMDD_HHMMSS.parquet.sha256

Configuration:
    BACKUP_SCHEDULE_CRON: Cron expression for backup schedule (default: "0 3 * * *")
    BACKUP_RETENTION_DAYS: Days to keep old backups (default: 14)

Usage:
    Triggered automatically by Airflow scheduler, or manually via Airflow UI.
"""
from __future__ import annotations
import os
import sys
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the project directory to Python path so we can import app modules.
# This is required because Airflow runs in a separate container with the
# project mounted at /opt/airflow/project.
PROJECT_MOUNT = "/opt/airflow/project"
if PROJECT_MOUNT not in sys.path:
    sys.path.append(PROJECT_MOUNT)


def _backup_postgres(**_):
    """
    Execute PostgreSQL backup and log results.

    Imports backup_postgres at runtime to avoid import errors during DAG parsing.
    Outputs JSON with backup file paths for Airflow task logs.
    """
    from app.backup_data import backup_postgres
    result = backup_postgres()
    print(json.dumps({"postgres_backup": result}, indent=2))


# DAG Definition
# The DAG context manager automatically registers all operators created within it.
with DAG(
    dag_id="daily_data_backup",
    description="Daily backup of PostgreSQL data (binary artifacts to /backups)",
    start_date=datetime(2025, 10, 1),
    schedule=os.getenv("BACKUP_SCHEDULE_CRON", "0 3 * * *"),
    catchup=False,  # Don't backfill missed runs
    tags=["backup", "data"],
    default_args={
        "owner": "airflow",
        "retries": 1,  # Retry once on failure
        "retry_delay": timedelta(minutes=5)
    },
) as dag:
    # Single task DAG - backs up the weather_data table to Parquet format
    t_pg = PythonOperator(task_id="backup_postgres", python_callable=_backup_postgres)
