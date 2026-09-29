FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9

WORKDIR /app

COPY spark_streaming_job.py .

CMD ["spark-submit", "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3","spark_streaming_job.py"]