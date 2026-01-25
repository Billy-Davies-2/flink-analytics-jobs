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
            "  httpRoute STRING," +
            "  windowStart BIGINT," +
            "  windowEnd BIGINT," +
            "  requestCount BIGINT," +
            "  successCount BIGINT," +
            "  clientErrorCount BIGINT," +
            "  serverErrorCount BIGINT," +
            "  totalBytesSent BIGINT," +
            "  totalBytesReceived BIGINT," +
            "  minLatencyMs BIGINT," +
            "  maxLatencyMs BIGINT," +
            "  avgLatencyMs DOUBLE," +
            "  p50LatencyMs DOUBLE," +
            "  p95LatencyMs DOUBLE," +
            "  p99LatencyMs DOUBLE," +
            "  errorRatePct DOUBLE," +
            "  processingTime BIGINT" +
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
            "  eventTime BIGINT," +
            "  httpRoute STRING," +
            "  hostname STRING," +
            "  `method` STRING," +
            "  `path` STRING," +
            "  statusCode INT," +
            "  errorCategory STRING," +
            "  responseTimeMs BIGINT," +
            "  upstreamCluster STRING," +
            "  clientIp STRING," +
            "  processingTime BIGINT" +
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
            "  alertTime BIGINT," +
            "  httpRoute STRING," +
            "  windowStart BIGINT," +
            "  windowEnd BIGINT," +
            "  p99LatencyMs DOUBLE," +
            "  p95LatencyMs DOUBLE," +
            "  avgLatencyMs DOUBLE," +
            "  thresholdMs BIGINT," +
            "  requestCount BIGINT," +
            "  errorRatePct DOUBLE," +
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

        // Convert DataStream to Table
        Table table = tableEnv.fromDataStream(stream, schema);

        // Add to statement set (will be executed later with execute())
        statementSet.addInsert(fullTableName, table);
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

        Table table = tableEnv.fromDataStream(stream, schema);
        
        // Add to statement set (will be executed later with execute())
        statementSet.addInsert(fullTableName, table);
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

        Table table = tableEnv.fromDataStream(stream, schema);
        
        // Add to statement set (will be executed later with execute())
        statementSet.addInsert(fullTableName, table);
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
