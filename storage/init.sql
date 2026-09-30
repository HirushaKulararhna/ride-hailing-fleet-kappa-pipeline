-- ==========================================================
-- Fleet Data Platform - Schema
-- Kappa architecture: one streaming pipeline feeds fleet_metrics
-- and alerts in near-real-time; the daily batch feed lands in
-- vehicle_costs and is reconciled into profitability_report by
-- an Airflow-orchestrated job.
-- ==========================================================

-- Raw-ish real-time fleet utilization metrics, windowed by Spark
-- Structured Streaming (append/update sink written every micro-batch).
CREATE TABLE IF NOT EXISTS fleet_metrics (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    zone            TEXT NOT NULL,
    active_vehicles INTEGER NOT NULL,
    idle_vehicles   INTEGER NOT NULL,
    trips_count     INTEGER NOT NULL,
    total_earnings  NUMERIC(12, 2) NOT NULL,
    avg_speed       NUMERIC(6, 2),
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fleet_metrics_window ON fleet_metrics (window_end);
CREATE INDEX IF NOT EXISTS idx_fleet_metrics_zone ON fleet_metrics (zone);

-- Per-vehicle earnings, windowed by Spark (separate from the per-zone
-- fleet_metrics aggregation). The daily profitability reconciliation joins
-- against this, since zone totals alone can't be split back out per vehicle.
CREATE TABLE IF NOT EXISTS vehicle_earnings (
    id            BIGSERIAL PRIMARY KEY,
    vehicle_id    TEXT NOT NULL,
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    earnings      NUMERIC(12, 2) NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vehicle_earnings_vehicle ON vehicle_earnings (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_earnings_window ON vehicle_earnings (window_start);

-- Per-vehicle idle-time tracking used for the "vehicle idle too long" alert
CREATE TABLE IF NOT EXISTS vehicle_status_latest (
    vehicle_id          TEXT PRIMARY KEY,
    last_status         TEXT NOT NULL,
    last_event_time     TIMESTAMP NOT NULL,
    idle_since          TIMESTAMP,
    zone                TEXT,
    updated_at          TIMESTAMP NOT NULL DEFAULT now()
);

-- Alerts raised by the streaming job or the watchdog
CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,       -- e.g. 'VEHICLE_IDLE', 'PIPELINE_STALE'
    severity     TEXT NOT NULL,       -- 'WARNING' | 'CRITICAL'
    subject      TEXT NOT NULL,       -- vehicle_id or 'pipeline'
    message      TEXT NOT NULL,
    raised_at    TIMESTAMP NOT NULL DEFAULT now(),
    resolved     BOOLEAN NOT NULL DEFAULT false
);

-- Daily batch feed: per-vehicle running costs from garages/fuel partners
CREATE TABLE IF NOT EXISTS vehicle_costs (
    id                 BIGSERIAL PRIMARY KEY,
    vehicle_id         TEXT NOT NULL,
    business_date      DATE NOT NULL,
    fuel_cost          NUMERIC(10, 2) NOT NULL,
    maintenance_cost   NUMERIC(10, 2) NOT NULL,
    distance_covered   NUMERIC(10, 2) NOT NULL,
    service_flag       BOOLEAN NOT NULL DEFAULT false,
    loaded_at          TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (vehicle_id, business_date)
);

-- Daily reconciliation output: streaming earnings joined with batch costs
CREATE TABLE IF NOT EXISTS profitability_report (
    id                  BIGSERIAL PRIMARY KEY,
    business_date       DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    total_earnings      NUMERIC(12, 2) NOT NULL,
    fuel_cost           NUMERIC(10, 2) NOT NULL,
    maintenance_cost    NUMERIC(10, 2) NOT NULL,
    net_profit          NUMERIC(12, 2) NOT NULL,
    is_unprofitable     BOOLEAN NOT NULL,
    generated_at        TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (business_date, vehicle_id)
);

-- Structured pipeline event log (used alongside container stdout logs)
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id          BIGSERIAL PRIMARY KEY,
    stage       TEXT NOT NULL,   -- 'ingestion' | 'processing' | 'storage' | 'orchestration'
    level       TEXT NOT NULL,   -- 'INFO' | 'WARNING' | 'ERROR'
    message     TEXT NOT NULL,
    logged_at   TIMESTAMP NOT NULL DEFAULT now()
);