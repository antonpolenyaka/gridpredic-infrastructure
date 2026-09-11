FROM apache/hive:4.0.1

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.6.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.367.jar

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar \
    /opt/hive/lib/postgres.jar