FROM apache/spark:4.0.4

USER root

RUN pip install --no-cache-dir "delta-spark==4.0.1"

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    /opt/spark/jars/hadoop-aws-3.4.1.jar

ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    /opt/spark/jars/aws-sdk-bundle-2.24.6.jar

WORKDIR /app