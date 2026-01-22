package com.homelab.flink;

import com.homelab.flink.common.IcebergCatalogConfig;
import com.homelab.flink.common.NatsJetStreamSourceConfig;
import com.homelab.flink.model.AccessLog;
import com.homelab.flink.model.ErrorEvent;
import com.homelab.flink.model.LatencyAlert;
import com.homelab.flink.model.RouteMetrics;
import com.homelab.flink.process.ErrorEventFilter;
import com.homelab.flink.process.LatencyAlertGenerator;
import com.homelab.flink.process.LatencyPercentileCalculator;
import com.homelab.flink.process.RouteMetricsAggregator;
import com.homelab.flink.sink.IcebergSinkFactory;
import com.homelab.flink.source.NatsJetStreamSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * HTTPRoute Real-Time Analytics Job
 * 
 * Processes Envoy Gateway access logs from NATS JetStream and produces aggregated 
 * metrics to Iceberg tables via Nessie catalog. This enables real-time visibility 
 * into HTTP traffic patterns, latency distributions, and error rates across HTTPRoutes.
 * 
 * Input: NATS JetStream stream (envoy.access.logs subject)
 * 
 * Output Tables:
 * - httproute_analytics.route_metrics_1m: Per-minute aggregations per route
 * - httproute_analytics.route_metrics_5m: 5-minute rolling aggregations
 * - httproute_analytics.error_events: Individual 4xx/5xx error events
 * - httproute_analytics.latency_alerts: P99 latency threshold breaches
 * 
 * Alternative: Consider using RisingWave for simpler SQL-based streaming analytics
 * if the processing requirements are straightforward.
 */
public class HTTPRouteRealtimeAnalytics {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPRouteRealtimeAnalytics.class);

    // ConfigOptions for type-safe configuration
    public static final ConfigOption<Integer> PARALLELISM = ConfigOptions
        .key("parallelism")
        .intType()
        .defaultValue(2)
        .withDescription("Job parallelism");

    public static final ConfigOption<Long> LATENCY_THRESHOLD = ConfigOptions
        .key("latency.threshold.ms")
        .longType()
        .defaultValue(500L)
        .withDescription("Latency threshold in milliseconds for alerts");

    public static final String DATABASE_NAME = "httproute_analytics";

    public static void main(String[] args) throws Exception {
        // Parse command line arguments into Configuration
        Configuration config = Configuration.fromMap(parseArgs(args));
        
        LOG.info("Starting HTTPRoute Real-Time Analytics job");
        LOG.info("Configuration: {}", config.toMap());

        // Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment(config);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Configure Iceberg catalog
        IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig(config);
        catalogConfig.registerCatalog(tableEnv);
        catalogConfig.createDatabaseIfNotExists(tableEnv, DATABASE_NAME);

        // Create Iceberg sink factory
        IcebergSinkFactory sinkFactory = new IcebergSinkFactory(tableEnv, DATABASE_NAME);
        
        // Ensure tables exist
        sinkFactory.createRouteMetrics1mTable();
        sinkFactory.createRouteMetrics5mTable();
        sinkFactory.createErrorEventsTable();
        sinkFactory.createLatencyAlertsTable();

        // Create NATS JetStream source
        NatsJetStreamSourceConfig natsConfig = new NatsJetStreamSourceConfig(config);
        LOG.info("NATS JetStream config: {}", natsConfig);
        
        NatsJetStreamSource natsSource = new NatsJetStreamSource(natsConfig);

        // Create watermark strategy for event-time processing
        WatermarkStrategy<AccessLog> watermarkStrategy = WatermarkStrategy
            .<AccessLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((log, recordTimestamp) -> log.getTimestampMillis())
            .withIdleness(Duration.ofMinutes(1));

        // Read from NATS JetStream source
        DataStream<AccessLog> accessLogs = env
            .fromSource(natsSource, watermarkStrategy, "NATS JetStream Source")
            .uid("nats-jetstream-source");

        // Get latency threshold for alerts
        long latencyThresholdMs = config.get(LATENCY_THRESHOLD);

        // Process 1-minute tumbling window aggregations
        SingleOutputStreamOperator<RouteMetrics> metrics1m = accessLogs
            .keyBy(AccessLog::getHttpRoute)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .aggregate(
                new RouteMetricsAggregator(),
                new LatencyPercentileCalculator()
            )
            .name("1-Minute Route Metrics Aggregation")
            .uid("route-metrics-1m-aggregation");

        // Write 1-minute metrics to Iceberg
        sinkFactory.addRouteMetricsSink(metrics1m, "route_metrics_1m");

        // Process 5-minute sliding window aggregations (1-minute slide)
        SingleOutputStreamOperator<RouteMetrics> metrics5m = accessLogs
            .keyBy(AccessLog::getHttpRoute)
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(5), Duration.ofMinutes(1)))
            .aggregate(
                new RouteMetricsAggregator(),
                new LatencyPercentileCalculator()
            )
            .name("5-Minute Rolling Route Metrics Aggregation")
            .uid("route-metrics-5m-aggregation");

        // Write 5-minute metrics to Iceberg
        sinkFactory.addRouteMetricsSink(metrics5m, "route_metrics_5m");

        // Filter and write error events (4xx/5xx)
        SingleOutputStreamOperator<ErrorEvent> errorEvents = accessLogs
            .filter(new ErrorEventFilter())
            .map(ErrorEvent::fromAccessLog)
            .name("Error Event Extraction")
            .uid("error-event-extraction");

        sinkFactory.addErrorEventsSink(errorEvents);

        // Generate latency alerts when P99 exceeds threshold
        SingleOutputStreamOperator<LatencyAlert> latencyAlerts = metrics1m
            .process(new LatencyAlertGenerator(latencyThresholdMs))
            .name("Latency Alert Generation")
            .uid("latency-alert-generation");

        sinkFactory.addLatencyAlertsSink(latencyAlerts);

        // Execute the job
        env.execute("HTTPRoute Real-Time Analytics");
    }

    /**
     * Parses command line arguments into a Map.
     */
    private static java.util.Map<String, String> parseArgs(String[] args) {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    map.put(parts[0], parts[1]);
                }
            }
        }
        return map;
    }

    /**
     * Creates and configures the Flink execution environment.
     */
    private static StreamExecutionEnvironment createExecutionEnvironment(Configuration config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        int parallelism = config.get(PARALLELISM);
        env.setParallelism(parallelism);

        // Configure checkpointing for exactly-once processing
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(60_000);
        checkpointConfig.setMinPauseBetweenCheckpoints(30_000);
        checkpointConfig.setCheckpointTimeout(120_000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        
        // Enable externalized checkpoints for recovery
        checkpointConfig.setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        );

        LOG.info("Execution environment configured with parallelism={}", parallelism);

        return env;
    }
}
