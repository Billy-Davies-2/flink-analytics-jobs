package com.homelab.flink;

import com.homelab.flink.model.AccessLog;
import com.homelab.flink.model.RouteMetrics;
import com.homelab.flink.process.LatencyPercentileCalculator;
import com.homelab.flink.process.RouteMetricsAggregator;
import com.homelab.flink.source.AccessLogDeserializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the HTTPRoute analytics pipeline using Flink MiniCluster.
 */
class HTTPRouteAnalyticsPipelineTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(2)
            .build()
    );

    @Test
    void shouldAggregateAccessLogs() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create test data
        List<AccessLog> testLogs = createTestAccessLogs();

        // Define watermark strategy
        WatermarkStrategy<AccessLog> watermarkStrategy = WatermarkStrategy
            .<AccessLog>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((log, ts) -> log.getTimestampMillis());

        // Create pipeline
        DataStream<RouteMetrics> metrics = env.fromCollection(testLogs)
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(AccessLog::getHttpRoute)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .aggregate(new RouteMetricsAggregator(), new LatencyPercentileCalculator());

        // Collect results
        List<RouteMetrics> results = new ArrayList<>();
        try (CloseableIterator<RouteMetrics> iterator = metrics.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }

        // Verify results
        assertThat(results).isNotEmpty();
        
        RouteMetrics productPageMetrics = results.stream()
            .filter(m -> "productpage".equals(m.getHttpRoute()))
            .findFirst()
            .orElse(null);

        assertThat(productPageMetrics).isNotNull();
        assertThat(productPageMetrics.getRequestCount()).isGreaterThan(0);
    }

    @Test
    void shouldDeserializeAndProcess() throws Exception {
        AccessLogDeserializer deserializer = new AccessLogDeserializer();
        
        String json = """
            {
                "timestamp": "2026-01-21T12:00:30Z",
                "http_route": "checkout",
                "hostname": "shop.example.com",
                "method": "POST",
                "path": "/checkout",
                "status_code": 200,
                "response_time_ms": 150,
                "bytes_sent": 512,
                "bytes_received": 1024,
                "upstream_cluster": "checkout-cluster",
                "client_ip": "10.42.1.50"
            }
            """;

        AccessLog log = deserializer.deserialize(json.getBytes());

        assertThat(log).isNotNull();
        assertThat(log.getHttpRoute()).isEqualTo("checkout");
        assertThat(log.getResponseTimeMs()).isEqualTo(150);
    }

    private List<AccessLog> createTestAccessLogs() {
        List<AccessLog> logs = new ArrayList<>();
        long baseTime = 1768996800000L; // 2026-01-21T12:00:00Z

        // Add logs for productpage route
        for (int i = 0; i < 10; i++) {
            logs.add(AccessLog.builder()
                .timestamp(java.time.Instant.ofEpochMilli(baseTime + i * 1000).toString())
                .httpRoute("productpage")
                .hostname("app.example.com")
                .method("GET")
                .path("/products")
                .statusCode(i < 8 ? 200 : 500)  // 20% error rate
                .responseTimeMs(50 + i * 10)
                .bytesSent(1024)
                .bytesReceived(256)
                .upstreamCluster("productpage-cluster")
                .clientIp("10.42.1." + i)
                .build());
        }

        // Add logs for checkout route
        for (int i = 0; i < 5; i++) {
            logs.add(AccessLog.builder()
                .timestamp(java.time.Instant.ofEpochMilli(baseTime + i * 2000).toString())
                .httpRoute("checkout")
                .hostname("app.example.com")
                .method("POST")
                .path("/checkout")
                .statusCode(200)
                .responseTimeMs(100 + i * 20)
                .bytesSent(512)
                .bytesReceived(2048)
                .upstreamCluster("checkout-cluster")
                .clientIp("10.42.2." + i)
                .build());
        }

        return logs;
    }
}
