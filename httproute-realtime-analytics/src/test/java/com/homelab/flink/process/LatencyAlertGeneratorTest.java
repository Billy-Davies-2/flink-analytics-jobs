package com.homelab.flink.process;

import com.homelab.flink.model.LatencyAlert;
import com.homelab.flink.model.RouteMetrics;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for LatencyAlertGenerator.
 */
class LatencyAlertGeneratorTest {

    private LatencyAlertGenerator alertGenerator;
    private Collector<LatencyAlert> collector;
    private static final long THRESHOLD_MS = 500;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        alertGenerator = new LatencyAlertGenerator(THRESHOLD_MS);
        collector = Mockito.mock(Collector.class);
    }

    @Test
    void shouldGenerateAlertWhenP99ExceedsThreshold() throws Exception {
        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .windowStart(1000L)
            .windowEnd(61000L)
            .requestCount(100)
            .p99LatencyMs(750.0)  // Above 500ms threshold
            .p95LatencyMs(600.0)
            .avgLatencyMs(300.0)
            .errorRatePct(1.5)
            .build();

        alertGenerator.processElement(metrics, null, collector);

        ArgumentCaptor<LatencyAlert> alertCaptor = ArgumentCaptor.forClass(LatencyAlert.class);
        verify(collector, times(1)).collect(alertCaptor.capture());

        LatencyAlert alert = alertCaptor.getValue();
        assertThat(alert.getHttpRoute()).isEqualTo("test-route");
        assertThat(alert.getP99LatencyMs()).isEqualTo(750.0);
        assertThat(alert.getThresholdMs()).isEqualTo(500);
        assertThat(alert.getSeverity()).isEqualTo("MEDIUM");
    }

    @Test
    void shouldNotGenerateAlertWhenP99BelowThreshold() throws Exception {
        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .requestCount(100)
            .p99LatencyMs(400.0)  // Below 500ms threshold
            .build();

        alertGenerator.processElement(metrics, null, collector);

        verify(collector, never()).collect(any());
    }

    @Test
    void shouldCalculateSeverityCorrectly() throws Exception {
        // Low severity: 1.0x - 1.5x threshold (500-750ms)
        verifyAlertSeverity(600.0, "LOW");

        // Medium severity: 1.5x - 2.0x threshold (750-1000ms)
        verifyAlertSeverity(800.0, "MEDIUM");

        // High severity: 2.0x - 3.0x threshold (1000-1500ms)
        verifyAlertSeverity(1200.0, "HIGH");

        // Critical severity: > 3.0x threshold (>1500ms)
        verifyAlertSeverity(2000.0, "CRITICAL");
    }

    @SuppressWarnings("unchecked")
    private void verifyAlertSeverity(double p99Latency, String expectedSeverity) throws Exception {
        Collector<LatencyAlert> localCollector = Mockito.mock(Collector.class);
        
        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .requestCount(100)
            .p99LatencyMs(p99Latency)
            .p95LatencyMs(p99Latency * 0.9)
            .avgLatencyMs(p99Latency * 0.5)
            .build();

        alertGenerator.processElement(metrics, null, localCollector);

        ArgumentCaptor<LatencyAlert> alertCaptor = ArgumentCaptor.forClass(LatencyAlert.class);
        verify(localCollector, times(1)).collect(alertCaptor.capture());

        assertThat(alertCaptor.getValue().getSeverity()).isEqualTo(expectedSeverity);
    }

    @Test
    void shouldHandleNullMetrics() throws Exception {
        alertGenerator.processElement(null, null, collector);
        verify(collector, never()).collect(any());
    }

    @Test
    void shouldHandleEmptyMetrics() throws Exception {
        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .requestCount(0)
            .p99LatencyMs(1000.0)
            .build();

        alertGenerator.processElement(metrics, null, collector);
        verify(collector, never()).collect(any());
    }

    @Test
    void shouldIncludeWindowInformation() throws Exception {
        long windowStart = 1705838400000L;
        long windowEnd = 1705838460000L;

        RouteMetrics metrics = RouteMetrics.builder()
            .httpRoute("test-route")
            .windowStart(windowStart)
            .windowEnd(windowEnd)
            .requestCount(100)
            .p99LatencyMs(600.0)
            .build();

        alertGenerator.processElement(metrics, null, collector);

        ArgumentCaptor<LatencyAlert> alertCaptor = ArgumentCaptor.forClass(LatencyAlert.class);
        verify(collector, times(1)).collect(alertCaptor.capture());

        LatencyAlert alert = alertCaptor.getValue();
        assertThat(alert.getWindowStart()).isEqualTo(windowStart);
        assertThat(alert.getWindowEnd()).isEqualTo(windowEnd);
    }
}
