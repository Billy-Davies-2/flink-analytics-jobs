package com.homelab.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * POJO representing a latency threshold breach alert.
 * 
 * This model captures information about when the P99 latency for a route
 * exceeds the configured threshold, enabling alerting and SLO monitoring.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LatencyAlert implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Alert timestamp in epoch milliseconds.
     */
    private long alertTime;

    /**
     * The HTTPRoute name experiencing high latency.
     */
    private String httpRoute;

    /**
     * Start of the measurement window.
     */
    private long windowStart;

    /**
     * End of the measurement window.
     */
    private long windowEnd;

    /**
     * P99 latency that triggered the alert (milliseconds).
     */
    private double p99LatencyMs;

    /**
     * P95 latency for context (milliseconds).
     */
    private double p95LatencyMs;

    /**
     * Average latency for context (milliseconds).
     */
    private double avgLatencyMs;

    /**
     * The threshold that was exceeded (milliseconds).
     */
    private long thresholdMs;

    /**
     * Number of requests in the window.
     */
    private long requestCount;

    /**
     * Error rate in the window (percentage).
     */
    private double errorRatePct;

    /**
     * Alert severity based on how much the threshold was exceeded.
     */
    private String severity;

    /**
     * Creates a LatencyAlert from RouteMetrics and threshold.
     */
    public static LatencyAlert fromMetrics(RouteMetrics metrics, long thresholdMs) {
        double exceedanceRatio = metrics.getP99LatencyMs() / thresholdMs;
        String severity = calculateSeverity(exceedanceRatio);

        return LatencyAlert.builder()
            .alertTime(System.currentTimeMillis())
            .httpRoute(metrics.getHttpRoute())
            .windowStart(metrics.getWindowStart())
            .windowEnd(metrics.getWindowEnd())
            .p99LatencyMs(metrics.getP99LatencyMs())
            .p95LatencyMs(metrics.getP95LatencyMs())
            .avgLatencyMs(metrics.getAvgLatencyMs())
            .thresholdMs(thresholdMs)
            .requestCount(metrics.getRequestCount())
            .errorRatePct(metrics.getErrorRatePct())
            .severity(severity)
            .build();
    }

    /**
     * Calculates alert severity based on how much the threshold was exceeded.
     */
    private static String calculateSeverity(double exceedanceRatio) {
        if (exceedanceRatio >= 3.0) {
            return "CRITICAL";
        } else if (exceedanceRatio >= 2.0) {
            return "HIGH";
        } else if (exceedanceRatio >= 1.5) {
            return "MEDIUM";
        } else {
            return "LOW";
        }
    }

    /**
     * Returns how many times the threshold was exceeded.
     */
    public double getExceedanceMultiple() {
        if (thresholdMs == 0) {
            return 0.0;
        }
        return p99LatencyMs / thresholdMs;
    }
}
