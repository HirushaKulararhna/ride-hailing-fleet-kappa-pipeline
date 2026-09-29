"""
Airflow DAG: daily batch ingestion + profitability reconciliation.

This is the orchestration layer required by the project brief for "managing
batch jobs or reporting pipelines". It does NOT reimplement stream
processing (that stays inside the single Spark Structured Streaming job,
consistent with the Kappa architecture decision) - it only:

  1. Detects the newest vehicle-cost file dropped by the batch producer.
  2. Loads/cleans it into the vehicle_costs table.
  3. Joins it against the already-materialized fleet_metrics (streaming
     output) to build the daily profitability_report.
  4. Runs a basic data-quality/health check and logs the outcome.

Simulated schedule: this DAG is triggered every 5 minutes to line up with
the batch producer's SIMULATED_DAY_SECONDS=300 default (1 simulated day =
5 real minutes). Adjust schedule_interval to match if you change that.
"""

import glob
import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("daily_batch_reconciliation_dag")

BATCH_DIR = "/data/batch"
PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "dbname": os.environ.get("POSTGRES_DB", "fleetdb"),
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleet"),
}

default_args = {
    "owner": "fleet-data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def _log_pipeline_event(stage: str, level: str, message: str) -> None:
    """Structured log written both to Airflow logs and pipeline_logs table."""
    log.info(json.dumps({"stage": stage, "level": level, "message": message}))
    try:
        conn = psycopg2.connect(**PG_CONN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_logs (stage, level, message) VALUES (%s, %s, %s)",
                (stage, level, message),
            )
        conn.close()
    except Exception as exc:  # noqa: BLE001
        log.error(f"Could not write to pipeline_logs: {exc}")


def find_latest_batch_file() -> str:
    files = sorted(glob.glob(os.path.join(BATCH_DIR, "vehicle_costs_*.json")))
    if not files:
        raise FileNotFoundError("No batch files found yet in /data/batch")
    return files[-1]


def load_vehicle_costs(**context):
    path = find_latest_batch_file()
    with open(path) as f:
        payload = json.load(f)

    business_date = payload["business_date"]
    records = payload["records"]

    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    loaded, skipped = 0, 0
    with conn.cursor() as cur:
        for r in records:
            # Basic data-quality cleaning: skip negative/nonsensical values
            # rather than silently loading bad data.
            if r["fuel_cost"] < 0 or r["maintenance_cost"] < 0 or r["distance_covered"] < 0:
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO vehicle_costs
                    (vehicle_id, business_date, fuel_cost, maintenance_cost, distance_covered, service_flag)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (vehicle_id, business_date) DO UPDATE SET
                    fuel_cost = EXCLUDED.fuel_cost,
                    maintenance_cost = EXCLUDED.maintenance_cost,
                    distance_covered = EXCLUDED.distance_covered,
                    service_flag = EXCLUDED.service_flag,
                    loaded_at = now();
                """,
                (
                    r["vehicle_id"],
                    business_date,
                    r["fuel_cost"],
                    r["maintenance_cost"],
                    r["distance_covered"],
                    r["service_flag"],
                ),
            )
            loaded += 1
    conn.close()

    _log_pipeline_event(
        "orchestration.load_vehicle_costs",
        "INFO",
        f"Loaded {loaded} vehicle cost records for {business_date} ({skipped} skipped as invalid) from {path}",
    )
    context["ti"].xcom_push(key="business_date", value=business_date)


def build_profitability_report(**context):
    business_date = context["ti"].xcom_pull(key="business_date", task_ids="load_vehicle_costs")
    if not business_date:
        raise ValueError("No business_date available from upstream task")

    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    simulated_day_seconds = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH earnings AS (
                SELECT vehicle_id, COALESCE(SUM(earnings), 0) AS total_earnings
                FROM vehicle_earnings
                WHERE window_start >= now() - interval '{simulated_day_seconds} seconds'
                GROUP BY vehicle_id
            )
            INSERT INTO profitability_report
                (business_date, vehicle_id, total_earnings, fuel_cost, maintenance_cost, net_profit, is_unprofitable)
            SELECT
                vc.business_date,
                vc.vehicle_id,
                COALESCE(e.total_earnings, 0),
                vc.fuel_cost,
                vc.maintenance_cost,
                (COALESCE(e.total_earnings, 0) - vc.fuel_cost - vc.maintenance_cost),
                (COALESCE(e.total_earnings, 0) - vc.fuel_cost - vc.maintenance_cost) < 0
            FROM vehicle_costs vc
            LEFT JOIN earnings e ON e.vehicle_id = vc.vehicle_id
            WHERE vc.business_date = %s
            ON CONFLICT (business_date, vehicle_id) DO UPDATE SET
                total_earnings = EXCLUDED.total_earnings,
                fuel_cost = EXCLUDED.fuel_cost,
                maintenance_cost = EXCLUDED.maintenance_cost,
                net_profit = EXCLUDED.net_profit,
                is_unprofitable = EXCLUDED.is_unprofitable,
                generated_at = now();
            """,
            (business_date,),
        )
        cur.execute(
            "SELECT count(*) FROM profitability_report WHERE business_date = %s AND is_unprofitable = true",
            (business_date,),
        )
        unprofitable_count = cur.fetchone()[0]
    conn.close()

    _log_pipeline_event(
        "orchestration.build_profitability_report",
        "INFO",
        f"Built profitability_report for {business_date}: {unprofitable_count} unprofitable vehicles",
    )


def health_check(**context):
    """Basic health-check rule: has the streaming pipeline written data recently?"""
    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        cur.execute("SELECT max(created_at) FROM fleet_metrics")
        last_metric_time = cur.fetchone()[0]
    conn.close()

    if last_metric_time is None:
        _log_pipeline_event("orchestration.health_check", "WARNING", "No fleet_metrics rows exist yet")
        return

    staleness = datetime.utcnow() - last_metric_time.replace(tzinfo=None)
    if staleness > timedelta(minutes=2):
        _log_pipeline_event(
            "orchestration.health_check",
            "WARNING",
            f"fleet_metrics stale by {staleness}. Streaming job may be down.",
        )
    else:
        _log_pipeline_event("orchestration.health_check", "INFO", f"Streaming pipeline healthy, last write {staleness} ago")


with DAG(
    dag_id="daily_batch_reconciliation",
    default_args=default_args,
    description="Load daily vehicle cost feed and reconcile against streaming earnings",
    schedule_interval=timedelta(minutes=5),  # matches SIMULATED_DAY_SECONDS=300
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "batch", "reconciliation"],
) as dag:

    load_costs = PythonOperator(
        task_id="load_vehicle_costs",
        python_callable=load_vehicle_costs,
    )

    reconcile = PythonOperator(
        task_id="build_profitability_report",
        python_callable=build_profitability_report,
    )

    check = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
    )

    load_costs >> reconcile >> check
