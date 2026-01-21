package com.homelab.flink.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for LatencyAlert.
 */
class LatencyAlertTest {

    @Test
    void shouldCreateFromMetrics() {
        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .windowStart(1000L)
            .windowEnd(61000L)
            .requestCount(100)
            .p99LatencyMs(750.0)
            .p95LatencyMs(600.0)
            .avgLatencyMs(300.0)
            .errorRatePct(2.5)
            .build();

        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);

        assertThat(alert.getHttpRoute()).isEqualTo("test-route");
        assertThat(alert.getWindowStart()).isEqualTo(1000L);
        assertThat(alert.getWindowEnd()).isEqualTo(61000L);
        assertThat(alert.getP99LatencyMs()).isEqualTo(750.0);
        assertThat(alert.getP95LatencyMs()).isEqualTo(600.0);
        assertThat(alert.getAvgLatencyMs()).isEqualTo(300.0);
        assertThat(alert.getThresholdMs()).isEqualTo(500);
        assertThat(alert.getRequestCount()).isEqualTo(100);
        assertThat(alert.getErrorRatePct()).isEqualTo(2.5);
    }

    @Test
    void shouldCalculateLowSeverity() {
        RouteMetrics metrics = createMetricsWithP99(600.0); // 1.2x threshold
        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);
        assertThat(alert.getSeverity()).isEqualTo("LOW");
    }

    @Test
    void shouldCalculateMediumSeverity() {
        RouteMetrics metrics = createMetricsWithP99(800.0); // 1.6x threshold
        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);
        assertThat(alert.getSeverity()).isEqualTo("MEDIUM");
    }

    @Test
    void shouldCalculateHighSeverity() {
        RouteMetrics metrics = createMetricsWithP99(1200.0); // 2.4x threshold
        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);
        assertThat(alert.getSeverity()).isEqualTo("HIGH");
    }

    @Test
    void shouldCalculateCriticalSeverity() {
        RouteMetrics metrics = createMetricsWithP99(2000.0); // 4x threshold
        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);
        assertThat(alert.getSeverity()).isEqualTo("CRITICAL");
    }

    @Test
    void shouldCalculateExceedanceMultiple() {
        RouteMetrics metrics = createMetricsWithP99(1000.0);
        LatencyAlert alert = LatencyAlert.fromMetrics(metrics, 500);
        
        assertThat(alert.getExceedanceMultiple()).isCloseTo(2.0, within(0.01));
    }

    private RouteMetrics createMetricsWithP99(double p99) {
        return RouteMetrics.builder()
            .httpRoute("test")
            .requestCount(100)
            .p99LatencyMs(p99)
            .build();
    }
}
