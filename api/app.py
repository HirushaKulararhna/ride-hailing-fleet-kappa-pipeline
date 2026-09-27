"""
Serving layer for the fleet data platform.

Exposes:
  GET /fleet/utilization       - real-time utilization metrics by zone
  GET /fleet/alerts            - active alerts (idle vehicles, pipeline health)
  GET /reports/profitability   - latest daily profitability reconciliation
  GET /health                  - liveness/readiness check
  GET /metrics                 - Prometheus-style metrics for observability
"""

import json
import logging
import os
import time

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps(
        {
            "ts": "%(asctime)s",
            "stage": "serving.api",
            "level": "%(levelname)s",
            "message": "%(message)s",
        }
    ),
)
log = logging.getLogger("fleet_api")

PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "dbname": os.environ.get("POSTGRES_DB", "fleetdb"),
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleet"),
}

app = FastAPI(title="Fleet Data Platform API")

REQUEST_COUNT = Counter("fleet_api_requests_total", "Total API requests", ["endpoint", "status"])
REQUEST_LATENCY = Histogram("fleet_api_request_latency_seconds", "Request latency", ["endpoint"])


def get_conn():
    return psycopg2.connect(**PG_CONN, cursor_factory=psycopg2.extras.RealDictCursor)


def timed_query(endpoint: str, query: str, params: tuple = ()):
    start = time.time()
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        conn.close()
        REQUEST_COUNT.labels(endpoint=endpoint, status="success").inc()
        return rows
    except Exception as exc:  # noqa: BLE001
        REQUEST_COUNT.labels(endpoint=endpoint, status="error").inc()
        log.error(f"Query failed for {endpoint}: {exc}")
        raise HTTPException(status_code=500, detail="Database query failed") from exc
    finally:
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(time.time() - start)


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"DB unreachable: {exc}") from exc


@app.get("/fleet/utilization")
def fleet_utilization():
    """Live fleet utilization: latest window per zone."""
    rows = timed_query(
        "fleet_utilization",
        """
        SELECT DISTINCT ON (zone)
            zone, window_start, window_end, active_vehicles, idle_vehicles,
            trips_count, total_earnings, avg_speed
        FROM fleet_metrics
        ORDER BY zone, window_end DESC
        """,
    )
    return {"zones": rows}


@app.get("/fleet/alerts")
def fleet_alerts():
    rows = timed_query(
        "fleet_alerts",
        """
        SELECT id, alert_type, severity, subject, message, raised_at, resolved
        FROM alerts
        WHERE resolved = false
        ORDER BY raised_at DESC
        LIMIT 50
        """,
    )
    return {"alerts": rows}


@app.get("/reports/profitability")
def profitability_report(business_date: str | None = None):
    if business_date:
        rows = timed_query(
            "profitability_report",
            """
            SELECT business_date, vehicle_id, total_earnings, fuel_cost,
                   maintenance_cost, net_profit, is_unprofitable
            FROM profitability_report
            WHERE business_date = %s
            ORDER BY net_profit ASC
            """,
            (business_date,),
        )
    else:
        rows = timed_query(
            "profitability_report",
            """
            SELECT business_date, vehicle_id, total_earnings, fuel_cost,
                   maintenance_cost, net_profit, is_unprofitable
            FROM profitability_report
            WHERE business_date = (SELECT max(business_date) FROM profitability_report)
            ORDER BY net_profit ASC
            """,
        )
    return {"report": rows}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
