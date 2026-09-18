FROM apache/spark:4.2.0

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "delta-spark==4.4.0" \
    -r /tmp/requirements.txt

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    /opt/spark/jars/hadoop-aws-3.5.0.jar

ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
    /opt/spark/jars/aws-sdk-bundle-2.35.4.jar

ADD https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
    /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar

WORKDIR /app