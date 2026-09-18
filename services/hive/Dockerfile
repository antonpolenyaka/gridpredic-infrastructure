FROM apache/hive:4.1.0

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.4.1.jar

ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    /opt/hadoop/share/hadoop/common/lib/aws-sdk-bundle-2.24.6.jar

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /opt/hive/lib/postgres.jar