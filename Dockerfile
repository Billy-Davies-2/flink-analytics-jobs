FROM flink:1.20.0-java21

# Copy the analytics job JARs
COPY httproute-realtime-analytics/target/httproute-realtime-analytics-*-shaded.jar /opt/flink/usrlib/httproute-realtime-analytics.jar

# Set default environment variables
ENV FLINK_PROPERTIES="taskmanager.numberOfTaskSlots: 2"

USER flink
