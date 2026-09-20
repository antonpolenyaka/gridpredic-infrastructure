FROM apache/airflow:slim-3.3.2-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY --from=spark requirements.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "apache-airflow-providers-apache-spark[pyspark]==6.3.2" \
    "pyspark==4.2.0" \
    "boto3==1.43.75" \
    -r /tmp/requirements-spark.txt

# The Ivy cache is mounted as a named volume. The directory must exist and
# belong to the airflow user, otherwise the volume is created as root and
# spark-submit cannot write to it.
USER root

RUN mkdir -p /home/airflow/.ivy2.5.2 \
    && chown airflow:root /home/airflow/.ivy2.5.2

USER airflow
