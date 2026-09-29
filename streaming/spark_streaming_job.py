"""
Spark Structured Streaming job - the single processing pipeline in this
project's Kappa architecture.

Reads raw trip telemetry events from Kafka, does two things per micro-batch:

  1. Windowed aggregation per zone (utilization/earnings) -> fleet_metrics
  2. Per-vehicle latest-status tracking (for the idle-vehicle alert) ->
     vehicle_status_latest

Both sinks are Postgres tables written via JDBC in foreachBatch, which is
the standard "queryable store" sink recommended for Structured Streaming
when you need upserts/aggregated writes rather than an append-only file
sink.

Because this is Kappa (not Lambda), there is no separate batch code path
for this same logic: the daily-batch reconciliation (joining against
vehicle_costs) is a *different* concern handled by the Airflow DAG, which
reads the already-materialized fleet_metrics table rather than
re-implementing trip aggregation.
"""

import json
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
    countDistinct,
    when,
    max as spark_max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format=json.dumps(
        {
            "ts": "%(asctime)s",
            "stage": "processing.spark_streaming_job",
            "level": "%(levelname)s",
            "message": "%(message)s",
        }
    ),
)
log = logging.getLogger("spark_streaming_job")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "trip_events")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/fleetdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "fleet")

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("fare", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("FleetKappaStreamingJob")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_events(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")).select("data.*")
    # Drop malformed events instead of letting the job crash - cleaning step
    # required by the assignment ("Transformations should be meaningful").
    return parsed.filter(col("vehicle_id").isNotNull() & col("timestamp").isNotNull())


def write_fleet_metrics(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    try:
        windowed = (
            batch_df.groupBy(window(col("timestamp"), "30 seconds"), col("zone"))
            .agg(
                countDistinct(when(col("status") != "idle", col("vehicle_id"))).alias("active_vehicles"),
                countDistinct(when(col("status") == "idle", col("vehicle_id"))).alias("idle_vehicles"),
                spark_count(when(col("status") == "on_trip", True)).alias("trips_count"),
                spark_sum("fare").alias("total_earnings"),
                spark_avg("speed").alias("avg_speed"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("zone"),
                col("active_vehicles"),
                col("idle_vehicles"),
                col("trips_count"),
                col("total_earnings"),
                col("avg_speed"),
            )
        )
        (
            windowed.write.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "fleet_metrics")
            .options(**JDBC_PROPS)
            .mode("append")
            .save()
        )
        log.info(f"Batch {batch_id}: wrote fleet_metrics rows")
    except Exception as exc:  # noqa: BLE001
        log.error(f"Batch {batch_id}: failed writing fleet_metrics - {exc}")


def write_vehicle_status(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    try:
        latest = (
            batch_df.groupBy("vehicle_id")
            .agg(
                spark_max("timestamp").alias("last_event_time"),
            )
        )
        # Join back to get the status/zone at that latest timestamp.
        latest_full = latest.join(
            batch_df, on=["vehicle_id"], how="inner"
        ).filter(col("timestamp") == col("last_event_time")).select(
            col("vehicle_id"),
            col("status").alias("last_status"),
            col("last_event_time"),
            col("zone"),
        ).dropDuplicates(["vehicle_id"])

        # Upsert via a staging table + merge would be ideal; for a mini-project
        # scale, delete-then-insert per batch keeps this simple and correct.
        staging_table = f"vehicle_status_staging_{batch_id}"
        (
            latest_full.write.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", staging_table)
            .options(**JDBC_PROPS)
            .mode("overwrite")
            .save()
        )

        import psycopg2  # local import: only needed on the driver

        conn = psycopg2.connect(
            host=POSTGRES_URL.split("//")[1].split(":")[0].split("/")[0],
            dbname="fleetdb",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO vehicle_status_latest (vehicle_id, last_status, last_event_time, zone, idle_since)
                SELECT vehicle_id, last_status, last_event_time, zone,
                       CASE WHEN last_status = 'idle' THEN last_event_time ELSE NULL END
                FROM {staging_table}
                ON CONFLICT (vehicle_id) DO UPDATE SET
                    last_status = EXCLUDED.last_status,
                    last_event_time = EXCLUDED.last_event_time,
                    zone = EXCLUDED.zone,
                    idle_since = CASE
                        WHEN EXCLUDED.last_status = 'idle' AND vehicle_status_latest.last_status = 'idle'
                            THEN vehicle_status_latest.idle_since
                        WHEN EXCLUDED.last_status = 'idle'
                            THEN EXCLUDED.last_event_time
                        ELSE NULL
                    END,
                    updated_at = now();
                DROP TABLE IF EXISTS {staging_table};
                """
            )
        conn.close()
        log.info(f"Batch {batch_id}: upserted vehicle_status_latest")
    except Exception as exc:  # noqa: BLE001
        log.error(f"Batch {batch_id}: failed writing vehicle_status_latest - {exc}")

def write_vehicle_earnings(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    try:
        vehicle_earnings = (
            batch_df.groupBy(window(col("timestamp"), "30 seconds"), col("vehicle_id"))
            .agg(spark_sum("fare").alias("earnings"))
            .select(
                col("vehicle_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("earnings"),
            )
        )
        (
            vehicle_earnings.write.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "vehicle_earnings")
            .options(**JDBC_PROPS)
            .mode("append")
            .save()
        )
        log.info(f"Batch {batch_id}: wrote vehicle_earnings rows")
    except Exception as exc:  # noqa: BLE001
        log.error(f"Batch {batch_id}: failed writing vehicle_earnings - {exc}")

def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark session started. Subscribing to '{TOPIC_NAME}' at {KAFKA_BOOTSTRAP}")

    events = read_events(spark)
    events_with_watermark = events.withWatermark("timestamp", "1 minute")

    metrics_query = (
        events_with_watermark.writeStream.foreachBatch(write_fleet_metrics)
        .option("checkpointLocation", "/tmp/checkpoints/fleet_metrics")
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    status_query = (
        events.writeStream.foreachBatch(write_vehicle_status)
        .option("checkpointLocation", "/tmp/checkpoints/vehicle_status")
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    earnings_query = (
        events_with_watermark.writeStream.foreachBatch(write_vehicle_earnings)
        .option("checkpointLocation", "/tmp/checkpoints/vehicle_earnings")
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    log.info("Streaming queries started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
