"""
Streaming source simulator (Ride-Hailing Fleet Operations).

Emits one GPS/trip telemetry event every EVENT_INTERVAL_SECONDS per active
vehicle, to a Kafka topic. This represents the continuous real-time source
required by the mini-project brief.

Event schema:
    trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Structured logging: every stage of the pipeline logs JSON so it can be
# aggregated/parsed centrally (observability requirement).
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format=json.dumps(
        {
            "ts": "%(asctime)s",
            "stage": "ingestion.streaming_producer",
            "level": "%(levelname)s",
            "message": "%(message)s",
        }
    ),
)
log = logging.getLogger("streaming_producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "trip_events")
EVENT_INTERVAL_SECONDS = float(os.environ.get("EVENT_INTERVAL_SECONDS", "2"))

ZONES = ["Downtown", "Airport", "Suburb_North", "Suburb_South", "Industrial_Park"]
STATUSES = ["idle", "enroute", "on_trip"]

# Simulated fleet: fixed set of vehicles/drivers so state (idle vehicles,
# trips) is coherent across events instead of pure noise.
NUM_VEHICLES = 20
VEHICLES = [f"VEH{100 + i}" for i in range(NUM_VEHICLES)]
DRIVERS = [f"DRV{200 + i}" for i in range(NUM_VEHICLES)]

# Rough bounding box for a fictional city, used to generate lat/lon.
CENTER_LAT, CENTER_LON = 6.9271, 79.8612  # Colombo-ish, purely illustrative


def connect_producer(retries: int = 20, delay: float = 3.0) -> KafkaProducer:
    """Retry connecting to Kafka since the broker may still be starting."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP} (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready yet (attempt {attempt}/{retries}), retrying in {delay}s")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")


class VehicleState:
    """Tracks each vehicle's current status/trip so events are coherent."""

    def __init__(self, vehicle_id: str, driver_id: str):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_id
        self.status = "idle"
        self.trip_id = None
        self.lat = CENTER_LAT + random.uniform(-0.05, 0.05)
        self.lon = CENTER_LON + random.uniform(-0.05, 0.05)
        self.zone = random.choice(ZONES)

    def step(self):
        # Simple state machine: idle -> enroute -> on_trip -> idle ...
        if self.status == "idle":
            if random.random() < 0.15:
                self.status = "enroute"
                self.trip_id = str(uuid.uuid4())
        elif self.status == "enroute":
            if random.random() < 0.4:
                self.status = "on_trip"
        elif self.status == "on_trip":
            if random.random() < 0.2:
                self.status = "idle"
                self.trip_id = None
                self.zone = random.choice(ZONES)

        # Small random walk for position
        self.lat += random.uniform(-0.002, 0.002)
        self.lon += random.uniform(-0.002, 0.002)

    def to_event(self) -> dict:
        speed = 0.0 if self.status == "idle" else round(random.uniform(10, 60), 1)
        fare = round(random.uniform(2, 5), 2) if self.status == "on_trip" else 0.0
        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": speed,
            "status": self.status,
            "zone": self.zone,
            "fare": fare,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def main():
    producer = connect_producer()
    vehicles = [VehicleState(v, d) for v, d in zip(VEHICLES, DRIVERS)]
    log.info(f"Starting streaming simulation for {len(vehicles)} vehicles -> topic '{TOPIC_NAME}'")

    event_count = 0
    try:
        while True:
            for vehicle in vehicles:
                vehicle.step()
                event = vehicle.to_event()
                producer.send(TOPIC_NAME, key=vehicle.vehicle_id, value=event)
                event_count += 1

            producer.flush()
            if event_count % 100 < len(vehicles):
                log.info(f"Published {event_count} events so far")

            time.sleep(EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Streaming producer stopped by user")
    except Exception as exc:  # noqa: BLE001
        log.error(f"Streaming producer crashed: {exc}")
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
