package com.homelab.flink.process;

import com.homelab.flink.model.RouteMetrics;
import com.homelab.flink.model.RouteMetricsAccumulator;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Window function that converts RouteMetricsAccumulator to RouteMetrics.
 * 
 * This function is applied after the RouteMetricsAggregator to finalize
 * the aggregation with window boundaries and calculate percentiles.
 */
public class LatencyPercentileCalculator 
    implements WindowFunction<RouteMetricsAccumulator, RouteMetrics, String, TimeWindow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LatencyPercentileCalculator.class);

    @Override
    public void apply(
            String httpRoute,
            TimeWindow window,
            Iterable<RouteMetricsAccumulator> input,
            Collector<RouteMetrics> out) throws Exception {

        // There should be exactly one accumulator from the aggregate function
        RouteMetricsAccumulator accumulator = input.iterator().next();

        if (accumulator == null || accumulator.getRequestCount() == 0) {
            LOG.debug("Empty window for route {}, skipping", httpRoute);
            return;
        }

        // Convert accumulator to RouteMetrics with window boundaries
        RouteMetrics metrics = accumulator.toRouteMetrics(window.getStart(), window.getEnd());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Window [{} - {}] for route {}: {} requests, avg latency={:.2f}ms, p99={:.2f}ms, error rate={:.2f}%",
                window.getStart(), window.getEnd(), httpRoute,
                metrics.getRequestCount(), metrics.getAvgLatencyMs(),
                metrics.getP99LatencyMs(), metrics.getErrorRatePct());
        }

        out.collect(metrics);
    }
}
