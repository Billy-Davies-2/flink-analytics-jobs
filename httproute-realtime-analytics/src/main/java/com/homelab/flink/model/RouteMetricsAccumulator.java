package com.homelab.flink.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Accumulator for aggregating access log events into route metrics.
 * 
 * This class maintains running statistics during window aggregation,
 * including latency samples for percentile calculation.
 */
@Data
public class RouteMetricsAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Maximum number of latency samples to retain for percentile calculation.
     * Uses reservoir sampling if exceeded.
     */
    private static final int MAX_LATENCY_SAMPLES = 10000;

    private String httpRoute;
    private long requestCount;
    private long successCount;
    private long clientErrorCount;
    private long serverErrorCount;
    private long totalBytesSent;
    private long totalBytesReceived;
    private long minLatencyMs;
    private long maxLatencyMs;
    private long sumLatencyMs;
    private List<Long> latencySamples;

    public RouteMetricsAccumulator() {
        this.requestCount = 0;
        this.successCount = 0;
        this.clientErrorCount = 0;
        this.serverErrorCount = 0;
        this.totalBytesSent = 0;
        this.totalBytesReceived = 0;
        this.minLatencyMs = Long.MAX_VALUE;
        this.maxLatencyMs = Long.MIN_VALUE;
        this.sumLatencyMs = 0;
        this.latencySamples = new ArrayList<>();
    }

    /**
     * Adds an access log event to the accumulator.
     */
    public void add(AccessLog log) {
        if (httpRoute == null) {
            httpRoute = log.getHttpRoute();
        }

        requestCount++;
        
        // Count by status category
        int statusCode = log.getStatusCode();
        if (statusCode >= 200 && statusCode < 300) {
            successCount++;
        } else if (statusCode >= 400 && statusCode < 500) {
            clientErrorCount++;
        } else if (statusCode >= 500) {
            serverErrorCount++;
        }

        // Accumulate bytes
        totalBytesSent += log.getBytesSent();
        totalBytesReceived += log.getBytesReceived();

        // Track latency statistics
        long latency = log.getResponseTimeMs();
        minLatencyMs = Math.min(minLatencyMs, latency);
        maxLatencyMs = Math.max(maxLatencyMs, latency);
        sumLatencyMs += latency;

        // Add latency sample (with reservoir sampling if needed)
        addLatencySample(latency);
    }

    /**
     * Adds a latency sample, using reservoir sampling if capacity is exceeded.
     */
    private void addLatencySample(long latency) {
        if (latencySamples.size() < MAX_LATENCY_SAMPLES) {
            latencySamples.add(latency);
        } else {
            // Reservoir sampling: replace a random element with probability MAX_LATENCY_SAMPLES/requestCount
            int replaceIndex = (int) (Math.random() * requestCount);
            if (replaceIndex < MAX_LATENCY_SAMPLES) {
                latencySamples.set(replaceIndex, latency);
            }
        }
    }

    /**
     * Merges another accumulator into this one.
     */
    public RouteMetricsAccumulator merge(RouteMetricsAccumulator other) {
        if (other == null || other.requestCount == 0) {
            return this;
        }

        if (this.httpRoute == null) {
            this.httpRoute = other.httpRoute;
        }

        this.requestCount += other.requestCount;
        this.successCount += other.successCount;
        this.clientErrorCount += other.clientErrorCount;
        this.serverErrorCount += other.serverErrorCount;
        this.totalBytesSent += other.totalBytesSent;
        this.totalBytesReceived += other.totalBytesReceived;
        this.minLatencyMs = Math.min(this.minLatencyMs, other.minLatencyMs);
        this.maxLatencyMs = Math.max(this.maxLatencyMs, other.maxLatencyMs);
        this.sumLatencyMs += other.sumLatencyMs;

        // Merge latency samples (simple approach: combine and truncate if needed)
        this.latencySamples.addAll(other.latencySamples);
        if (this.latencySamples.size() > MAX_LATENCY_SAMPLES) {
            // Randomly sample down to MAX_LATENCY_SAMPLES
            java.util.Collections.shuffle(this.latencySamples);
            this.latencySamples = new ArrayList<>(
                this.latencySamples.subList(0, MAX_LATENCY_SAMPLES)
            );
        }

        return this;
    }

    /**
     * Returns the average latency in milliseconds.
     */
    public double getAverageLatencyMs() {
        if (requestCount == 0) {
            return 0.0;
        }
        return (double) sumLatencyMs / requestCount;
    }

    /**
     * Calculates a specific percentile from the latency samples.
     * 
     * @param percentile The percentile to calculate (0-100)
     * @return The latency value at the specified percentile
     */
    public double getLatencyPercentile(double percentile) {
        if (latencySamples.isEmpty()) {
            return 0.0;
        }

        List<Long> sorted = new ArrayList<>(latencySamples);
        java.util.Collections.sort(sorted);

        double index = (percentile / 100.0) * (sorted.size() - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);

        if (lowerIndex == upperIndex) {
            return sorted.get(lowerIndex);
        }

        // Linear interpolation
        double fraction = index - lowerIndex;
        return sorted.get(lowerIndex) + fraction * (sorted.get(upperIndex) - sorted.get(lowerIndex));
    }

    /**
     * Creates a RouteMetrics from this accumulator.
     */
    public RouteMetrics toRouteMetrics(long windowStart, long windowEnd) {
        double errorRate = 0.0;
        if (requestCount > 0) {
            errorRate = ((double) (clientErrorCount + serverErrorCount) / requestCount) * 100.0;
        }

        return RouteMetrics.builder()
            .httpRoute(httpRoute)
            .windowStart(windowStart)
            .windowEnd(windowEnd)
            .requestCount(requestCount)
            .successCount(successCount)
            .clientErrorCount(clientErrorCount)
            .serverErrorCount(serverErrorCount)
            .totalBytesSent(totalBytesSent)
            .totalBytesReceived(totalBytesReceived)
            .minLatencyMs(requestCount > 0 ? minLatencyMs : 0)
            .maxLatencyMs(requestCount > 0 ? maxLatencyMs : 0)
            .avgLatencyMs(getAverageLatencyMs())
            .p50LatencyMs(getLatencyPercentile(50))
            .p95LatencyMs(getLatencyPercentile(95))
            .p99LatencyMs(getLatencyPercentile(99))
            .errorRatePct(errorRate)
            .processingTime(System.currentTimeMillis())
            .build();
    }
}
