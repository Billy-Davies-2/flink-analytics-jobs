package com.homelab.flink.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for RouteMetrics.
 */
class RouteMetricsTest {

    @Test
    void shouldCalculateRequestsPerSecond() {
        RouteMetrics metrics = RouteMetrics.builder()
            .requestCount(120)
            .windowStart(0)
            .windowEnd(60000)  // 60 seconds
            .build();

        assertThat(metrics.getRequestsPerSecond()).isCloseTo(2.0, within(0.01));
    }

    @Test
    void shouldCalculateThroughput() {
        RouteMetrics metrics = RouteMetrics.builder()
            .totalBytesSent(60000)
            .totalBytesReceived(30000)
            .windowStart(0)
            .windowEnd(60000)  // 60 seconds
            .build();

        assertThat(metrics.getThroughputBytesSentPerSecond()).isCloseTo(1000.0, within(0.01));
        assertThat(metrics.getThroughputBytesReceivedPerSecond()).isCloseTo(500.0, within(0.01));
    }

    @Test
    void shouldHandleZeroDuration() {
        RouteMetrics metrics = RouteMetrics.builder()
            .requestCount(100)
            .windowStart(1000)
            .windowEnd(1000)  // Zero duration
            .build();

        assertThat(metrics.getRequestsPerSecond()).isEqualTo(0.0);
    }

    @Test
    void shouldCalculateErrorRate() {
        RouteMetrics metrics = RouteMetrics.builder()
            .requestCount(100)
            .clientErrorCount(5)
            .serverErrorCount(3)
            .build();

        assertThat(metrics.calculateErrorRate()).isCloseTo(8.0, within(0.01));
    }

    @Test
    void shouldGetWindowDuration() {
        RouteMetrics metrics = RouteMetrics.builder()
            .windowStart(0)
            .windowEnd(300000)  // 5 minutes
            .build();

        assertThat(metrics.getWindowDurationSeconds()).isEqualTo(300);
    }
}
