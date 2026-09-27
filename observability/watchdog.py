"""
Observability watchdog.

Implements the two minimum alert/health-check rules required by the brief:

  1. Pipeline staleness: no new fleet_metrics rows in STALE_THRESHOLD_SECONDS
     -> raises a PIPELINE_STALE alert (streaming job or Kafka likely down).
  2. Vehicle idle too long: a vehicle has been idle beyond IDLE_THRESHOLD_SECONDS
     -> raises a VEHICLE_IDLE alert (per the suggested outputs in the brief).

Runs as its own long-lived container so it keeps checking independently of
the streaming/batch pipelines - if those go down, the watchdog is what
notices and records it.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps(
        {
            "ts": "%(asctime)s",
            "stage": "observability.watchdog",
            "level": "%(levelname)s",
            "message": "%(message)s",
        }
    ),
)
log = logging.getLogger("watchdog")

PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "dbname": os.environ.get("POSTGRES_DB", "fleetdb"),
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleet"),
}

STALE_THRESHOLD_SECONDS = int(os.environ.get("STALE_THRESHOLD_SECONDS", "60"))
IDLE_THRESHOLD_SECONDS = int(os.environ.get("IDLE_THRESHOLD_SECONDS", "120"))
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", "20"))


def get_conn(retries=20, delay=3):
    for attempt in range(retries):
        try:
            return psycopg2.connect(**PG_CONN)
        except psycopg2.OperationalError:
            log.warning(f"Postgres not ready (attempt {attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")


def raise_alert(conn, alert_type: str, severity: str, subject: str, message: str):
    with conn.cursor() as cur:
        # Avoid spamming duplicate unresolved alerts for the same subject/type
        cur.execute(
            """
            SELECT id FROM alerts
            WHERE alert_type = %s AND subject = %s AND resolved = false
            """,
            (alert_type, subject),
        )
        if cur.fetchone():
            return
        cur.execute(
            """
            INSERT INTO alerts (alert_type, severity, subject, message)
            VALUES (%s, %s, %s, %s)
            """,
            (alert_type, severity, subject, message),
        )
    log.warning(f"ALERT [{severity}] {alert_type} - {subject}: {message}")


def resolve_alert(conn, alert_type: str, subject: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE alerts SET resolved = true
            WHERE alert_type = %s AND subject = %s AND resolved = false
            """,
            (alert_type, subject),
        )


def check_pipeline_staleness(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT max(created_at) FROM fleet_metrics")
        (last_write,) = cur.fetchone()

    if last_write is None:
        return  # pipeline hasn't produced anything yet, nothing to alert on

    staleness = datetime.now(timezone.utc) - last_write.replace(tzinfo=timezone.utc)
    if staleness > timedelta(seconds=STALE_THRESHOLD_SECONDS):
        raise_alert(
            conn,
            "PIPELINE_STALE",
            "CRITICAL",
            "pipeline",
            f"No fleet_metrics written in {int(staleness.total_seconds())}s "
            f"(threshold {STALE_THRESHOLD_SECONDS}s). Check the Spark streaming job and Kafka.",
        )
    else:
        resolve_alert(conn, "PIPELINE_STALE", "pipeline")


def check_idle_vehicles(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT vehicle_id, idle_since
            FROM vehicle_status_latest
            WHERE last_status = 'idle' AND idle_since IS NOT NULL
            """
        )
        rows = cur.fetchall()

    now = datetime.now(timezone.utc)
    for vehicle_id, idle_since in rows:
        idle_seconds = (now - idle_since.replace(tzinfo=timezone.utc)).total_seconds()
        if idle_seconds > IDLE_THRESHOLD_SECONDS:
            raise_alert(
                conn,
                "VEHICLE_IDLE",
                "WARNING",
                vehicle_id,
                f"Vehicle {vehicle_id} idle for {int(idle_seconds)}s (threshold {IDLE_THRESHOLD_SECONDS}s).",
            )
        else:
            resolve_alert(conn, "VEHICLE_IDLE", vehicle_id)


def main():
    log.info(
        f"Watchdog starting. stale_threshold={STALE_THRESHOLD_SECONDS}s "
        f"idle_threshold={IDLE_THRESHOLD_SECONDS}s interval={CHECK_INTERVAL_SECONDS}s"
    )
    conn = get_conn()
    conn.autocommit = True
    while True:
        try:
            check_pipeline_staleness(conn)
            check_idle_vehicles(conn)
        except Exception as exc:  # noqa: BLE001
            log.error(f"Watchdog check failed: {exc}")
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            conn = get_conn()
            conn.autocommit = True
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
