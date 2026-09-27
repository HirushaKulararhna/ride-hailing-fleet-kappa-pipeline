# Fleet Data Platform — Ride-Hailing Fleet Operations (Kappa Architecture)

EC8202 Big Data Analytics — Mini Project (University of Ruhuna)

## 1. Architecture Summary

This project implements a **Kappa architecture**: a single stream-processing
pipeline (Kafka -> Spark Structured Streaming -> Postgres) handles all
event processing. The daily-batch cost feed is treated as a slow-arriving
side input rather than a parallel batch codepath — it's loaded and joined
against the already-materialized streaming output by an Airflow DAG, not
recomputed through a separate processing engine.

**Why Kappa over Lambda for this use case:**
- The core metric (fleet utilization/earnings) only needs one source of
  truth and one query logic, whether the underlying event is "fresh" or
  replayed from Kafka retention — a second batch codepath would duplicate
  logic without a clear benefit.
- Reprocessing needs are met by Kafka's log retention + Spark's
  checkpointing, without maintaining two codebases.
- The daily reconciliation isn't really a "batch recompute" of the same
  aggregation — it's a distinct join against a different, slower-changing
  dataset, which fits naturally as an orchestrated downstream job rather
  than requiring Lambda's dual-path design.

*(Expand this into your report's `Architecture Decision` section — discuss
latency, replay, cost, and consistency trade-offs explicitly, and note
Lambda as the rejected alternative with reasoning.)*

```
                     ┌─────────────────────┐
 streaming_producer  │                     │
 (GPS/trip events)   │        Kafka        │
 ───────────────────►│   topic: trip_events│
                      └──────────┬──────────┘
                                 │
                                 ▼
                   ┌───────────────────────────┐
                   │  Spark Structured Streaming│
                   │  - windowed zone metrics   │
                   │  - per-vehicle latest state│
                   └─────────────┬──────────────┘
                                 │ JDBC
                                 ▼
                      ┌────────────────────┐
                      │      Postgres       │
                      │ fleet_metrics        │
                      │ vehicle_status_latest│
                      │ alerts               │
                      │ vehicle_costs         │◄──┐
                      │ profitability_report  │   │
                      └─────────┬──────────────┘   │
                                │                   │ load + reconcile
                    ┌───────────┴──────────┐        │
                    ▼                      │  ┌─────┴──────┐
              FastAPI (serving)            └──┤  Airflow    │
              /fleet/utilization               │  DAG        │◄── batch_producer
              /fleet/alerts                     │ (5-min sim  │    (daily cost file)
              /reports/profitability            │  schedule)  │
              /metrics (Prometheus)             └────────────┘

              watchdog (independent) ── polls Postgres ── raises alerts
```

## 2. Technology Stack & Justification

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Apache Kafka (KRaft, single broker) | Decoupled pub/sub buffer, partition by `vehicle_id`/`zone` for ordered per-vehicle events, retention enables Kappa-style replay |
| Processing | Spark Structured Streaming | One engine for windowed aggregation and stateful per-vehicle tracking, avoids maintaining a second batch engine |
| Orchestration | Apache Airflow | Manages the daily reconciliation job (dependency ordering, retries, scheduling) — separate concern from stream processing |
| Storage/Serving | PostgreSQL | Relational joins (earnings vs costs) are natural in SQL; easy to query for the API and for the report screenshots |
| Serving API | FastAPI | Lightweight, exposes both business endpoints and a `/metrics` endpoint for observability |

## 3. Simulated Clock

- **Streaming events**: emitted every `EVENT_INTERVAL_SECONDS` (default 2s) per vehicle.
- **1 simulated "day"** = `SIMULATED_DAY_SECONDS` (default 300s = 5 minutes). The batch producer drops one cost file per simulated day; the Airflow DAG runs on a matching 5-minute schedule.
- State this compression explicitly in your report.

## 4. Running It

Requires Docker + Docker Compose, and internet access (Spark downloads the
Kafka/Postgres connector JARs on first run via `--packages`).

```bash
docker compose up --build
```

This starts, in order of dependency:
1. `kafka`, `postgres` (with schema auto-applied from `storage/init.sql`)
2. `streaming-producer`, `batch-producer`
3. `spark-job` (consumes Kafka, writes to Postgres)
4. `api` on http://localhost:8000 (docs at `/docs`)
5. `airflow-standalone` on http://localhost:8080 (user: `admin` / `admin`) — enable the `daily_batch_reconciliation` DAG from the UI
6. `watchdog` (raises alerts into the `alerts` table)

**First run note:** the Spark job takes a minute or two to download
connector JARs and initialize; watch `docker compose logs -f spark-job`.

### Useful endpoints
- `GET http://localhost:8000/fleet/utilization` — live per-zone metrics
- `GET http://localhost:8000/fleet/alerts` — active alerts
- `GET http://localhost:8000/reports/profitability` — latest daily reconciliation
- `GET http://localhost:8000/metrics` — Prometheus-format metrics
- `GET http://localhost:8000/health` — liveness check

### Demoing the alert rules
- **Vehicle idle alert**: wait ~2 minutes; some simulated vehicles will sit idle past `IDLE_THRESHOLD_SECONDS` and appear in `/fleet/alerts`.
- **Pipeline staleness alert**: `docker compose stop spark-job`, wait past `STALE_THRESHOLD_SECONDS` (default 60s), check `/fleet/alerts` for a `PIPELINE_STALE` entry, then `docker compose start spark-job` and confirm it auto-resolves.

## 5. Observability

- **Structured JSON logging** at every stage (`ingestion.*`, `processing.*`, `orchestration.*`, `serving.*`) — visible via `docker compose logs`, and orchestration events are additionally persisted to the `pipeline_logs` table.
- **Metrics**: `/metrics` exposes request counts and latency histograms (Prometheus format) from the API; extend this by pointing a Prometheus instance at it if you want a Grafana dashboard for the report screenshots.
- **Alert rules** (minimum 1 required, this project has 2): idle-vehicle threshold, pipeline-staleness threshold — both implemented in `observability/watchdog.py` and persisted to the `alerts` table so they're queryable, not just log lines.

## 6. What's Left for You to Do

This scaffold is a working starting point, not the finished submission —
you and your teammate still need to:
- Run it end-to-end and fix any environment-specific issues (JAR download
  speed, container resource limits, etc.)
- Tune the aggregation windows / thresholds and justify your choices
- Build the report: diagrams, screenshots of `/fleet/utilization`, the
  Airflow DAG graph view, and the alerts firing
- Add automated tests (e.g. `pytest` around the batch producer's cleaning
  logic, or a Spark unit test on the aggregation)
- Practice explaining every line of `spark_streaming_job.py` and the DAG —
  demo + viva is 40% of your module grade
- Write the individual contributions statement

## 7. Repo Structure

```
fleet-pipeline/
├── docker-compose.yml
├── storage/init.sql                        # Postgres schema
├── producers/
│   ├── streaming_producer.py               # GPS/trip event simulator -> Kafka
│   └── batch_producer.py                   # Daily vehicle-cost file simulator
├── streaming/
│   └── spark_streaming_job.py              # Core Kappa processing job
├── airflow/dags/
│   └── daily_batch_reconciliation_dag.py   # Batch load + profitability join
├── api/
│   └── app.py                              # Serving layer + /metrics
└── observability/
    └── watchdog.py                         # Alert/health-check rules
```

