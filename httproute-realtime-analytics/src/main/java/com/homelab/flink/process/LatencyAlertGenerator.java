package com.homelab.flink.process;

import com.homelab.flink.model.LatencyAlert;
import com.homelab.flink.model.RouteMetrics;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process function that generates latency alerts when P99 exceeds a threshold.
 * 
 * This function monitors the P99 latency from route metrics and emits alerts
 * when the configured threshold is exceeded, enabling SLO monitoring and alerting.
 */
public class LatencyAlertGenerator extends ProcessFunction<RouteMetrics, LatencyAlert> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LatencyAlertGenerator.class);

    private final long thresholdMs;

    /**
     * Creates a LatencyAlertGenerator with the specified threshold.
     *
     * @param thresholdMs The P99 latency threshold in milliseconds
     */
    public LatencyAlertGenerator(long thresholdMs) {
        this.thresholdMs = thresholdMs;
    }

    @Override
    public void processElement(
            RouteMetrics metrics,
            ProcessFunction<RouteMetrics, LatencyAlert>.Context ctx,
            Collector<LatencyAlert> out) throws Exception {

        if (metrics == null || metrics.getRequestCount() == 0) {
            return;
        }

        // Check if P99 latency exceeds threshold
        if (metrics.getP99LatencyMs() > thresholdMs) {
            LatencyAlert alert = LatencyAlert.fromMetrics(metrics, thresholdMs);

            LOG.warn("Latency alert for route {}: P99={}ms exceeds threshold={}ms (severity: {})",
                metrics.getHttpRoute(),
                String.format("%.2f", metrics.getP99LatencyMs()),
                thresholdMs,
                alert.getSeverity());

            out.collect(alert);
        }
    }
}
