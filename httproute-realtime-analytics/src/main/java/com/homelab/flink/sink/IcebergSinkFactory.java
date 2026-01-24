package com.homelab.flink.sink;

import com.homelab.flink.model.ErrorEvent;
import com.homelab.flink.model.LatencyAlert;
import com.homelab.flink.model.RouteMetrics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Iceberg table sinks for analytics data.
 * 
 * This factory manages the creation of Iceberg tables and the connection
 * of Flink DataStreams to those tables for writing analytics results.
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    private final StreamTableEnvironment tableEnv;
    private final String database;

    /**
     * Creates an IcebergSinkFactory for the specified database.
     *
     * @param tableEnv The Flink StreamTableEnvironment
     * @param database The target database name
     */
    public IcebergSinkFactory(StreamTableEnvironment tableEnv, String database) {
        this.tableEnv = tableEnv;
        this.database = database;
    }

    /**
     * Creates the route_metrics_1m table if it doesn't exist.
     */
    public void createRouteMetrics1mTable() {
        createRouteMetricsTable("route_metrics_1m");
    }

    /**
     * Creates the route_metrics_5m table if it doesn't exist.
     */
    public void createRouteMetrics5mTable() {
        createRouteMetricsTable("route_metrics_5m");
    }

    /**
     * Creates a route metrics table with the given name.
     */
    private void createRouteMetricsTable(String tableName) {
        String fullTableName = database + "." + tableName;
        LOG.info("Creating table {} if not exists", fullTableName);

        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "  http_route STRING," +
            "  window_start TIMESTAMP(3)," +
            "  window_end TIMESTAMP(3)," +
            "  request_count BIGINT," +
            "  success_count BIGINT," +
            "  client_error_count BIGINT," +
            "  server_error_count BIGINT," +
            "  total_bytes_sent BIGINT," +
            "  total_bytes_received BIGINT," +
            "  min_latency_ms BIGINT," +
            "  max_latency_ms BIGINT," +
            "  avg_latency_ms DOUBLE," +
            "  p50_latency_ms DOUBLE," +
            "  p95_latency_ms DOUBLE," +
            "  p99_latency_ms DOUBLE," +
            "  error_rate_pct DOUBLE," +
            "  processing_time TIMESTAMP(3)" +
            ") PARTITIONED BY (window_end)",
            fullTableName
        );

        tableEnv.executeSql(createTableSql);
    }

    /**
     * Creates the error_events table if it doesn't exist.
     */
    public void createErrorEventsTable() {
        String fullTableName = database + ".error_events";
        LOG.info("Creating table {} if not exists", fullTableName);

        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "  event_time TIMESTAMP(3)," +
            "  http_route STRING," +
            "  hostname STRING," +
            "  method STRING," +
            "  path STRING," +
            "  status_code INT," +
            "  error_category STRING," +
            "  response_time_ms BIGINT," +
            "  upstream_cluster STRING," +
            "  client_ip STRING," +
            "  processing_time TIMESTAMP(3)" +
            ") PARTITIONED BY (event_time, error_category)",
            fullTableName
        );

        tableEnv.executeSql(createTableSql);
    }

    /**
     * Creates the latency_alerts table if it doesn't exist.
     */
    public void createLatencyAlertsTable() {
        String fullTableName = database + ".latency_alerts";
        LOG.info("Creating table {} if not exists", fullTableName);

        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "  alert_time TIMESTAMP(3)," +
            "  http_route STRING," +
            "  window_start TIMESTAMP(3)," +
            "  window_end TIMESTAMP(3)," +
            "  p99_latency_ms DOUBLE," +
            "  p95_latency_ms DOUBLE," +
            "  avg_latency_ms DOUBLE," +
            "  threshold_ms BIGINT," +
            "  request_count BIGINT," +
            "  error_rate_pct DOUBLE," +
            "  severity STRING" +
            ") PARTITIONED BY (alert_time, severity)",
            fullTableName
        );

        tableEnv.executeSql(createTableSql);
    }

    /**
     * Adds an Iceberg sink for RouteMetrics to the specified table.
     *
     * @param stream The DataStream of RouteMetrics
     * @param tableName The target table name (without database prefix)
     */
    public void addRouteMetricsSink(DataStream<RouteMetrics> stream, String tableName) {
        String fullTableName = database + "." + tableName;
        LOG.info("Adding sink for RouteMetrics to table {}", fullTableName);

        // Define schema for RouteMetrics
        Schema schema = Schema.newBuilder()
            .column("http_route", DataTypes.STRING())
            .column("window_start", DataTypes.TIMESTAMP(3))
            .column("window_end", DataTypes.TIMESTAMP(3))
            .column("request_count", DataTypes.BIGINT())
            .column("success_count", DataTypes.BIGINT())
            .column("client_error_count", DataTypes.BIGINT())
            .column("server_error_count", DataTypes.BIGINT())
            .column("total_bytes_sent", DataTypes.BIGINT())
            .column("total_bytes_received", DataTypes.BIGINT())
            .column("min_latency_ms", DataTypes.BIGINT())
            .column("max_latency_ms", DataTypes.BIGINT())
            .column("avg_latency_ms", DataTypes.DOUBLE())
            .column("p50_latency_ms", DataTypes.DOUBLE())
            .column("p95_latency_ms", DataTypes.DOUBLE())
            .column("p99_latency_ms", DataTypes.DOUBLE())
            .column("error_rate_pct", DataTypes.DOUBLE())
            .column("processing_time", DataTypes.TIMESTAMP(3))
            .build();

        // Convert DataStream to Table
        Table table = tableEnv.fromDataStream(stream, schema);

        // Insert into Iceberg table
        table.executeInsert(fullTableName);
    }

    /**
     * Adds an Iceberg sink for ErrorEvents.
     *
     * @param stream The DataStream of ErrorEvent
     */
    public void addErrorEventsSink(DataStream<ErrorEvent> stream) {
        String fullTableName = database + ".error_events";
        LOG.info("Adding sink for ErrorEvents to table {}", fullTableName);

        Schema schema = Schema.newBuilder()
            .column("event_time", DataTypes.TIMESTAMP(3))
            .column("http_route", DataTypes.STRING())
            .column("hostname", DataTypes.STRING())
            .column("method", DataTypes.STRING())
            .column("path", DataTypes.STRING())
            .column("status_code", DataTypes.INT())
            .column("error_category", DataTypes.STRING())
            .column("response_time_ms", DataTypes.BIGINT())
            .column("upstream_cluster", DataTypes.STRING())
            .column("client_ip", DataTypes.STRING())
            .column("processing_time", DataTypes.TIMESTAMP(3))
            .build();

        Table table = tableEnv.fromDataStream(stream, schema);
        table.executeInsert(fullTableName);
    }

    /**
     * Adds an Iceberg sink for LatencyAlerts.
     *
     * @param stream The DataStream of LatencyAlert
     */
    public void addLatencyAlertsSink(DataStream<LatencyAlert> stream) {
        String fullTableName = database + ".latency_alerts";
        LOG.info("Adding sink for LatencyAlerts to table {}", fullTableName);

        Schema schema = Schema.newBuilder()
            .column("alert_time", DataTypes.TIMESTAMP(3))
            .column("http_route", DataTypes.STRING())
            .column("window_start", DataTypes.TIMESTAMP(3))
            .column("window_end", DataTypes.TIMESTAMP(3))
            .column("p99_latency_ms", DataTypes.DOUBLE())
            .column("p95_latency_ms", DataTypes.DOUBLE())
            .column("avg_latency_ms", DataTypes.DOUBLE())
            .column("threshold_ms", DataTypes.BIGINT())
            .column("request_count", DataTypes.BIGINT())
            .column("error_rate_pct", DataTypes.DOUBLE())
            .column("severity", DataTypes.STRING())
            .build();

        Table table = tableEnv.fromDataStream(stream, schema);
        table.executeInsert(fullTableName);
    }
}
