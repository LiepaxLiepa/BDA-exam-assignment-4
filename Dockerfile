FROM python:3.11-bookworm

LABEL description="AIS vessel collision detection with PySpark"

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV DATA_DIR=/data
ENV OUTPUT_DIR=/output
ENV INPUT_GLOB=*.csv
ENV START_TS="2021-12-01 00:00:00"
ENV END_TS="2021-12-31 23:59:59"
ENV SPARK_LOCAL_DIRS=/tmp/spark-local

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ /app/src/
RUN mkdir -p /data /output /tmp/spark-local

CMD ["python", "/app/src/collision_detection.py"]
