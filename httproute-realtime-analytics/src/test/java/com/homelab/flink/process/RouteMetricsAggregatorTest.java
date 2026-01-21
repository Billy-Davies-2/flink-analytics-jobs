package com.homelab.flink.process;

import com.homelab.flink.model.AccessLog;
import com.homelab.flink.model.RouteMetricsAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for RouteMetricsAggregator.
 */
class RouteMetricsAggregatorTest {

    private RouteMetricsAggregator aggregator;

    @BeforeEach
    void setUp() {
        aggregator = new RouteMetricsAggregator();
    }

    @Test
    void shouldCreateEmptyAccumulator() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();

        assertThat(acc).isNotNull();
        assertThat(acc.getRequestCount()).isZero();
    }

    @Test
    void shouldAddAccessLogToAccumulator() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();
        AccessLog log = createAccessLog("test-route", 200, 50);

        acc = aggregator.add(log, acc);

        assertThat(acc.getRequestCount()).isEqualTo(1);
        assertThat(acc.getHttpRoute()).isEqualTo("test-route");
        assertThat(acc.getSuccessCount()).isEqualTo(1);
    }

    @Test
    void shouldCountStatusCategories() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();

        acc = aggregator.add(createAccessLog("route", 200, 10), acc);
        acc = aggregator.add(createAccessLog("route", 201, 10), acc);
        acc = aggregator.add(createAccessLog("route", 400, 10), acc);
        acc = aggregator.add(createAccessLog("route", 404, 10), acc);
        acc = aggregator.add(createAccessLog("route", 500, 10), acc);

        assertThat(acc.getRequestCount()).isEqualTo(5);
        assertThat(acc.getSuccessCount()).isEqualTo(2);
        assertThat(acc.getClientErrorCount()).isEqualTo(2);
        assertThat(acc.getServerErrorCount()).isEqualTo(1);
    }

    @Test
    void shouldTrackLatencyStatistics() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();

        acc = aggregator.add(createAccessLog("route", 200, 10), acc);
        acc = aggregator.add(createAccessLog("route", 200, 50), acc);
        acc = aggregator.add(createAccessLog("route", 200, 100), acc);

        assertThat(acc.getMinLatencyMs()).isEqualTo(10);
        assertThat(acc.getMaxLatencyMs()).isEqualTo(100);
        assertThat(acc.getAverageLatencyMs()).isCloseTo(53.33, within(0.01));
    }

    @Test
    void shouldCalculatePercentiles() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();

        // Add 100 requests with latencies 1-100ms
        for (int i = 1; i <= 100; i++) {
            acc = aggregator.add(createAccessLog("route", 200, i), acc);
        }

        assertThat(acc.getLatencyPercentile(50)).isCloseTo(50.0, within(1.0));
        assertThat(acc.getLatencyPercentile(95)).isCloseTo(95.0, within(1.0));
        assertThat(acc.getLatencyPercentile(99)).isCloseTo(99.0, within(1.0));
    }

    @Test
    void shouldMergeAccumulators() {
        RouteMetricsAccumulator acc1 = aggregator.createAccumulator();
        acc1 = aggregator.add(createAccessLog("route", 200, 10), acc1);
        acc1 = aggregator.add(createAccessLog("route", 200, 20), acc1);

        RouteMetricsAccumulator acc2 = aggregator.createAccumulator();
        acc2 = aggregator.add(createAccessLog("route", 500, 30), acc2);
        acc2 = aggregator.add(createAccessLog("route", 404, 40), acc2);

        RouteMetricsAccumulator merged = aggregator.merge(acc1, acc2);

        assertThat(merged.getRequestCount()).isEqualTo(4);
        assertThat(merged.getSuccessCount()).isEqualTo(2);
        assertThat(merged.getClientErrorCount()).isEqualTo(1);
        assertThat(merged.getServerErrorCount()).isEqualTo(1);
        assertThat(merged.getMinLatencyMs()).isEqualTo(10);
        assertThat(merged.getMaxLatencyMs()).isEqualTo(40);
    }

    @Test
    void shouldHandleNullAccessLog() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();
        acc = aggregator.add(null, acc);

        assertThat(acc.getRequestCount()).isZero();
    }

    @Test
    void shouldAccumulateBytes() {
        RouteMetricsAccumulator acc = aggregator.createAccumulator();

        AccessLog log1 = AccessLog.builder()
            .httpRoute("route")
            .statusCode(200)
            .responseTimeMs(10)
            .bytesSent(1000)
            .bytesReceived(500)
            .build();

        AccessLog log2 = AccessLog.builder()
            .httpRoute("route")
            .statusCode(200)
            .responseTimeMs(20)
            .bytesSent(2000)
            .bytesReceived(1000)
            .build();

        acc = aggregator.add(log1, acc);
        acc = aggregator.add(log2, acc);

        assertThat(acc.getTotalBytesSent()).isEqualTo(3000);
        assertThat(acc.getTotalBytesReceived()).isEqualTo(1500);
    }

    private AccessLog createAccessLog(String httpRoute, int statusCode, long responseTimeMs) {
        return AccessLog.builder()
            .timestamp("2026-01-21T12:00:00Z")
            .httpRoute(httpRoute)
            .hostname("test.example.com")
            .method("GET")
            .path("/test")
            .statusCode(statusCode)
            .responseTimeMs(responseTimeMs)
            .bytesSent(1024)
            .bytesReceived(256)
            .upstreamCluster("test-cluster")
            .clientIp("10.0.0.1")
            .build();
    }
}
