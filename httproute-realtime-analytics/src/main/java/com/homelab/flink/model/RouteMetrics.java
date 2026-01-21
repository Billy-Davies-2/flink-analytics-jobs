package com.homelab.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * POJO representing aggregated metrics for an HTTPRoute over a time window.
 * 
 * This model captures request counts, latency statistics, error rates, and
 * throughput metrics computed from a window of access log events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RouteMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The HTTPRoute name.
     */
    private String httpRoute;

    /**
     * Start of the aggregation window (epoch milliseconds).
     */
    private long windowStart;

    /**
     * End of the aggregation window (epoch milliseconds).
     */
    private long windowEnd;

    /**
     * Total number of requests in the window.
     */
    private long requestCount;

    /**
     * Number of successful requests (2xx responses).
     */
    private long successCount;

    /**
     * Number of client error responses (4xx).
     */
    private long clientErrorCount;

    /**
     * Number of server error responses (5xx).
     */
    private long serverErrorCount;

    /**
     * Total bytes sent across all responses.
     */
    private long totalBytesSent;

    /**
     * Total bytes received across all requests.
     */
    private long totalBytesReceived;

    /**
     * Minimum response time in milliseconds.
     */
    private long minLatencyMs;

    /**
     * Maximum response time in milliseconds.
     */
    private long maxLatencyMs;

    /**
     * Average response time in milliseconds.
     */
    private double avgLatencyMs;

    /**
     * 50th percentile (median) response time in milliseconds.
     */
    private double p50LatencyMs;

    /**
     * 95th percentile response time in milliseconds.
     */
    private double p95LatencyMs;

    /**
     * 99th percentile response time in milliseconds.
     */
    private double p99LatencyMs;

    /**
     * Error rate as a percentage (0-100).
     */
    private double errorRatePct;

    /**
     * Processing timestamp when these metrics were computed.
     */
    private long processingTime;

    /**
     * Calculates the error rate as a percentage.
     */
    public double calculateErrorRate() {
        if (requestCount == 0) {
            return 0.0;
        }
        return ((double) (clientErrorCount + serverErrorCount) / requestCount) * 100.0;
    }

    /**
     * Returns the window start as an Instant.
     */
    public Instant getWindowStartInstant() {
        return Instant.ofEpochMilli(windowStart);
    }

    /**
     * Returns the window end as an Instant.
     */
    public Instant getWindowEndInstant() {
        return Instant.ofEpochMilli(windowEnd);
    }

    /**
     * Returns the window duration in seconds.
     */
    public long getWindowDurationSeconds() {
        return (windowEnd - windowStart) / 1000;
    }

    /**
     * Calculates requests per second for this window.
     */
    public double getRequestsPerSecond() {
        long durationSeconds = getWindowDurationSeconds();
        if (durationSeconds == 0) {
            return 0.0;
        }
        return (double) requestCount / durationSeconds;
    }

    /**
     * Calculates throughput in bytes per second (sent).
     */
    public double getThroughputBytesSentPerSecond() {
        long durationSeconds = getWindowDurationSeconds();
        if (durationSeconds == 0) {
            return 0.0;
        }
        return (double) totalBytesSent / durationSeconds;
    }

    /**
     * Calculates throughput in bytes per second (received).
     */
    public double getThroughputBytesReceivedPerSecond() {
        long durationSeconds = getWindowDurationSeconds();
        if (durationSeconds == 0) {
            return 0.0;
        }
        return (double) totalBytesReceived / durationSeconds;
    }
}
