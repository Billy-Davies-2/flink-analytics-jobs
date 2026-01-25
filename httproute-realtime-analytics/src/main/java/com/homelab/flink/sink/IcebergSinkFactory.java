package com.homelab.flink.sink;

import com.homelab.flink.model.ErrorEvent;
import com.homelab.flink.model.LatencyAlert;
import com.homelab.flink.model.RouteMetrics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Iceberg table sinks for analytics data.
 * 
 * This factory manages the creation of Iceberg tables and the connection
 * of Flink DataStreams to those tables for writing analytics results.
 * 
 * Uses StatementSet to batch all INSERT operations into a single execute() call,
 * which is required for Flink's application mode.
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    private final StreamTableEnvironment tableEnv;
    private final String database;
    private final StatementSet statementSet;

    /**
     * Creates an IcebergSinkFactory for the specified database.
     *
     * @param tableEnv The Flink StreamTableEnvironment
     * @param database The target database name
     */
    public IcebergSinkFactory(StreamTableEnvironment tableEnv, String database) {
        this.tableEnv = tableEnv;
        this.database = database;
        this.statementSet = tableEnv.createStatementSet();
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
            "  window_start TIMESTAMP(6)," +
            "  window_end TIMESTAMP(6)," +
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
            "  processing_time TIMESTAMP(6)" +
            ")",
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
            "  event_time TIMESTAMP(6)," +
            "  http_route STRING," +
            "  hostname STRING," +
            "  `method` STRING," +
            "  `path` STRING," +
            "  status_code INT," +
            "  error_category STRING," +
            "  response_time_ms BIGINT," +
            "  upstream_cluster STRING," +
            "  client_ip STRING," +
            "  processing_time TIMESTAMP(6)" +
            ")",
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
            "  alert_time TIMESTAMP(6)," +
            "  http_route STRING," +
            "  window_start TIMESTAMP(6)," +
            "  window_end TIMESTAMP(6)," +
            "  p99_latency_ms DOUBLE," +
            "  p95_latency_ms DOUBLE," +
            "  avg_latency_ms DOUBLE," +
            "  threshold_ms BIGINT," +
            "  request_count BIGINT," +
            "  error_rate_pct DOUBLE," +
            "  severity STRING" +
            ")",
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
        String tempViewName = "route_metrics_" + tableName.replace("route_metrics_", "") + "_temp";
        LOG.info("Adding sink for RouteMetrics to table {}", fullTableName);

        // Define schema for RouteMetrics (using BIGINT for epoch millis timestamps)
        Schema schema = Schema.newBuilder()
            .column("httpRoute", DataTypes.STRING())
            .column("windowStart", DataTypes.BIGINT())
            .column("windowEnd", DataTypes.BIGINT())
            .column("requestCount", DataTypes.BIGINT())
            .column("successCount", DataTypes.BIGINT())
            .column("clientErrorCount", DataTypes.BIGINT())
            .column("serverErrorCount", DataTypes.BIGINT())
            .column("totalBytesSent", DataTypes.BIGINT())
            .column("totalBytesReceived", DataTypes.BIGINT())
            .column("minLatencyMs", DataTypes.BIGINT())
            .column("maxLatencyMs", DataTypes.BIGINT())
            .column("avgLatencyMs", DataTypes.DOUBLE())
            .column("p50LatencyMs", DataTypes.DOUBLE())
            .column("p95LatencyMs", DataTypes.DOUBLE())
            .column("p99LatencyMs", DataTypes.DOUBLE())
            .column("errorRatePct", DataTypes.DOUBLE())
            .column("processingTime", DataTypes.BIGINT())
            .build();

        // Convert DataStream to Table and register as temporary view
        Table table = tableEnv.fromDataStream(stream, schema);
        tableEnv.createTemporaryView(tempViewName, table);

        // Build INSERT SQL with BIGINT to TIMESTAMP conversion and column renaming
        String insertSql = String.format(
            "INSERT INTO %s " +
            "SELECT " +
            "  httpRoute AS http_route, " +
            "  TO_TIMESTAMP_LTZ(windowStart, 3) AS window_start, " +
            "  TO_TIMESTAMP_LTZ(windowEnd, 3) AS window_end, " +
            "  requestCount AS request_count, " +
            "  successCount AS success_count, " +
            "  clientErrorCount AS client_error_count, " +
            "  serverErrorCount AS server_error_count, " +
            "  totalBytesSent AS total_bytes_sent, " +
            "  totalBytesReceived AS total_bytes_received, " +
            "  minLatencyMs AS min_latency_ms, " +
            "  maxLatencyMs AS max_latency_ms, " +
            "  avgLatencyMs AS avg_latency_ms, " +
            "  p50LatencyMs AS p50_latency_ms, " +
            "  p95LatencyMs AS p95_latency_ms, " +
            "  p99LatencyMs AS p99_latency_ms, " +
            "  errorRatePct AS error_rate_pct, " +
            "  TO_TIMESTAMP_LTZ(processingTime, 3) AS processing_time " +
            "FROM %s",
            fullTableName, tempViewName
        );

        // Add to statement set (will be executed later with execute())
        statementSet.addInsertSql(insertSql);
    }

    /**
     * Adds an Iceberg sink for ErrorEvents.
     *
     * @param stream The DataStream of ErrorEvent
     */
    public void addErrorEventsSink(DataStream<ErrorEvent> stream) {
        String fullTableName = database + ".error_events";
        String tempViewName = "error_events_temp";
        LOG.info("Adding sink for ErrorEvents to table {}", fullTableName);

        Schema schema = Schema.newBuilder()
            .column("eventTime", DataTypes.BIGINT())
            .column("httpRoute", DataTypes.STRING())
            .column("hostname", DataTypes.STRING())
            .column("method", DataTypes.STRING())
            .column("path", DataTypes.STRING())
            .column("statusCode", DataTypes.INT())
            .column("errorCategory", DataTypes.STRING())
            .column("responseTimeMs", DataTypes.BIGINT())
            .column("upstreamCluster", DataTypes.STRING())
            .column("clientIp", DataTypes.STRING())
            .column("processingTime", DataTypes.BIGINT())
            .build();

        // Convert DataStream to Table and register as temporary view
        Table table = tableEnv.fromDataStream(stream, schema);
        tableEnv.createTemporaryView(tempViewName, table);

        // Build INSERT SQL with BIGINT to TIMESTAMP conversion and column renaming
        String insertSql = String.format(
            "INSERT INTO %s " +
            "SELECT " +
            "  TO_TIMESTAMP_LTZ(eventTime, 3) AS event_time, " +
            "  httpRoute AS http_route, " +
            "  hostname, " +
            "  `method`, " +
            "  `path`, " +
            "  statusCode AS status_code, " +
            "  errorCategory AS error_category, " +
            "  responseTimeMs AS response_time_ms, " +
            "  upstreamCluster AS upstream_cluster, " +
            "  clientIp AS client_ip, " +
            "  TO_TIMESTAMP_LTZ(processingTime, 3) AS processing_time " +
            "FROM %s",
            fullTableName, tempViewName
        );

        // Add to statement set (will be executed later with execute())
        statementSet.addInsertSql(insertSql);
    }

    /**
     * Adds an Iceberg sink for LatencyAlerts.
     *
     * @param stream The DataStream of LatencyAlert
     */
    public void addLatencyAlertsSink(DataStream<LatencyAlert> stream) {
        String fullTableName = database + ".latency_alerts";
        String tempViewName = "latency_alerts_temp";
        LOG.info("Adding sink for LatencyAlerts to table {}", fullTableName);

        Schema schema = Schema.newBuilder()
            .column("alertTime", DataTypes.BIGINT())
            .column("httpRoute", DataTypes.STRING())
            .column("windowStart", DataTypes.BIGINT())
            .column("windowEnd", DataTypes.BIGINT())
            .column("p99LatencyMs", DataTypes.DOUBLE())
            .column("p95LatencyMs", DataTypes.DOUBLE())
            .column("avgLatencyMs", DataTypes.DOUBLE())
            .column("thresholdMs", DataTypes.BIGINT())
            .column("requestCount", DataTypes.BIGINT())
            .column("errorRatePct", DataTypes.DOUBLE())
            .column("severity", DataTypes.STRING())
            .build();

        // Convert DataStream to Table and register as temporary view
        Table table = tableEnv.fromDataStream(stream, schema);
        tableEnv.createTemporaryView(tempViewName, table);

        // Build INSERT SQL with BIGINT to TIMESTAMP conversion and column renaming
        String insertSql = String.format(
            "INSERT INTO %s " +
            "SELECT " +
            "  TO_TIMESTAMP_LTZ(alertTime, 3) AS alert_time, " +
            "  httpRoute AS http_route, " +
            "  TO_TIMESTAMP_LTZ(windowStart, 3) AS window_start, " +
            "  TO_TIMESTAMP_LTZ(windowEnd, 3) AS window_end, " +
            "  p99LatencyMs AS p99_latency_ms, " +
            "  p95LatencyMs AS p95_latency_ms, " +
            "  avgLatencyMs AS avg_latency_ms, " +
            "  thresholdMs AS threshold_ms, " +
            "  requestCount AS request_count, " +
            "  errorRatePct AS error_rate_pct, " +
            "  severity " +
            "FROM %s",
            fullTableName, tempViewName
        );

        // Add to statement set (will be executed later with execute())
        statementSet.addInsertSql(insertSql);
    }

    /**
     * Executes all registered sink operations.
     * 
     * This must be called after all sinks have been added. It batches all INSERT
     * operations into a single execute() call, which is required for Flink's
     * application mode (cannot have multiple execute() calls).
     */
    public void execute() {
        LOG.info("Executing all Iceberg sink operations");
        statementSet.execute();
    }
}
