"""
Daily-batch source simulator (Ride-Hailing Fleet Operations).

Drops one JSON file per simulated "day" into BATCH_OUTPUT_DIR, representing
the once-daily vehicle expense feed from garages/fuel partners. Airflow
picks these files up and loads them into Postgres (see
airflow/dags/daily_batch_reconciliation_dag.py).

Simulated time: one "day" = SIMULATED_DAY_SECONDS (default 300s = 5 min).
State this compression clearly in the report.

File schema (per row):
    vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag
"""

import json
import logging
import os
import random
import time
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps(
        {
            "ts": "%(asctime)s",
            "stage": "ingestion.batch_producer",
            "level": "%(levelname)s",
            "message": "%(message)s",
        }
    ),
)
log = logging.getLogger("batch_producer")

BATCH_OUTPUT_DIR = os.environ.get("BATCH_OUTPUT_DIR", "/data/batch")
SIMULATED_DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))

NUM_VEHICLES = 20
VEHICLES = [f"VEH{100 + i}" for i in range(NUM_VEHICLES)]

# Start the simulated calendar at "today" and advance one simulated day per
# SIMULATED_DAY_SECONDS interval so the reconciliation DAG has a business_date
# to join against.
START_DATE = date.today()


def generate_daily_file(business_date: date) -> str:
    os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(BATCH_OUTPUT_DIR, f"vehicle_costs_{business_date.isoformat()}.json")

    records = []
    for vehicle_id in VEHICLES:
        distance = round(random.uniform(50, 300), 1)
        # Fuel cost roughly scales with distance; maintenance is occasional.
        fuel_cost = round(distance * random.uniform(0.15, 0.35), 2)
        service_flag = random.random() < 0.1
        maintenance_cost = round(random.uniform(50, 400), 2) if service_flag else round(random.uniform(0, 20), 2)
        records.append(
            {
                "vehicle_id": vehicle_id,
                "fuel_cost": fuel_cost,
                "maintenance_cost": maintenance_cost,
                "distance_covered": distance,
                "service_flag": service_flag,
            }
        )

    payload = {"business_date": business_date.isoformat(), "records": records}
    tmp_path = filename + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(payload, f, indent=2)
    os.rename(tmp_path, filename)  # atomic-ish "drop" so readers never see a partial file
    return filename


def main():
    log.info(
        f"Starting daily batch simulation. 1 simulated day = {SIMULATED_DAY_SECONDS}s. "
        f"Output dir: {BATCH_OUTPUT_DIR}"
    )
    day_offset = 0
    try:
        while True:
            business_date = START_DATE + timedelta(days=day_offset)
            path = generate_daily_file(business_date)
            log.info(f"Dropped daily batch file for {business_date.isoformat()} -> {path}")
            day_offset += 1
            time.sleep(SIMULATED_DAY_SECONDS)
    except KeyboardInterrupt:
        log.info("Batch producer stopped by user")
    except Exception as exc:  # noqa: BLE001
        log.error(f"Batch producer crashed: {exc}")
        raise


if __name__ == "__main__":
    main()
