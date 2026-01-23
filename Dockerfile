FROM flink:2.0.1-java21

# Copy the analytics job JAR to lib/ so Iceberg/Hadoop classes are on system classpath
# This is required because FlinkCatalogFactory is loaded by the system classloader
COPY httproute-realtime-analytics/target/httproute-realtime-analytics-*-shaded.jar /opt/flink/lib/httproute-realtime-analytics.jar

# Set default environment variables
ENV FLINK_PROPERTIES="taskmanager.numberOfTaskSlots: 2"

USER flink
