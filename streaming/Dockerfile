FROM apache/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir psycopg2-binary

WORKDIR /app
COPY spark_streaming_job.py .

# --packages triggers Maven download of the Kafka + Postgres connectors on
# first run (requires internet access from the container).
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "spark_streaming_job.py"]
