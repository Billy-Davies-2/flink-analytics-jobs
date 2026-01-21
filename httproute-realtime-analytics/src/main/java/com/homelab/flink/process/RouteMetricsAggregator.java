package com.homelab.flink.process;

import com.homelab.flink.model.AccessLog;
import com.homelab.flink.model.RouteMetricsAccumulator;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aggregate function for computing route metrics from access log events.
 * 
 * This function incrementally aggregates access logs within a window,
 * computing counts, latency statistics, and error rates.
 */
public class RouteMetricsAggregator implements AggregateFunction<AccessLog, RouteMetricsAccumulator, RouteMetricsAccumulator> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RouteMetricsAggregator.class);

    @Override
    public RouteMetricsAccumulator createAccumulator() {
        return new RouteMetricsAccumulator();
    }

    @Override
    public RouteMetricsAccumulator add(AccessLog log, RouteMetricsAccumulator accumulator) {
        if (log == null) {
            return accumulator;
        }
        
        accumulator.add(log);
        return accumulator;
    }

    @Override
    public RouteMetricsAccumulator getResult(RouteMetricsAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public RouteMetricsAccumulator merge(RouteMetricsAccumulator a, RouteMetricsAccumulator b) {
        if (a == null || a.getRequestCount() == 0) {
            return b;
        }
        if (b == null || b.getRequestCount() == 0) {
            return a;
        }
        return a.merge(b);
    }
}
