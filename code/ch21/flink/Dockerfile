# Flink 2.2 with the Kafka connector for Flink SQL and the lab's job
FROM flink:2.2.1-java17
ADD --chown=flink:flink https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
COPY windows.sql /opt/flink/windows.sql
