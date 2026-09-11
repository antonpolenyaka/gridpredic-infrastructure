FROM apache/airflow:slim-3.3.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    "apache-airflow-providers-apache-spark[pyspark]==6.3.2" \
    "pyspark==4.0.4"