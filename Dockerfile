FROM flink:2.0.1-java21

# Copy the analytics job JAR
COPY httproute-realtime-analytics/target/httproute-realtime-analytics-*-shaded.jar /opt/flink/usrlib/httproute-realtime-analytics.jar

# Set default environment variables including classloader configuration for Iceberg
ENV FLINK_PROPERTIES="taskmanager.numberOfTaskSlots: 2\nclassloader.resolve-order: child-first"

USER flink
