package com.homelab.flink.process;

import com.homelab.flink.model.AccessLog;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate function for session-based analytics.
 * 
 * Groups requests by client IP within a session window and computes
 * session-level statistics like page views, duration, and error counts.
 */
public class SessionAggregator implements AggregateFunction<AccessLog, SessionAggregator.SessionAccumulator, SessionAggregator.SessionMetrics> {

    private static final long serialVersionUID = 1L;

    @Override
    public SessionAccumulator createAccumulator() {
        return new SessionAccumulator();
    }

    @Override
    public SessionAccumulator add(AccessLog log, SessionAccumulator accumulator) {
        if (log == null) {
            return accumulator;
        }

        accumulator.clientIp = log.getClientIp();
        accumulator.requestCount++;
        
        if (accumulator.sessionStart == 0 || log.getTimestampMillis() < accumulator.sessionStart) {
            accumulator.sessionStart = log.getTimestampMillis();
        }
        if (log.getTimestampMillis() > accumulator.sessionEnd) {
            accumulator.sessionEnd = log.getTimestampMillis();
        }

        accumulator.totalResponseTimeMs += log.getResponseTimeMs();
        accumulator.totalBytesSent += log.getBytesSent();

        if (log.isError()) {
            accumulator.errorCount++;
        }

        // Track unique paths visited
        accumulator.pathCounts.merge(log.getPath(), 1, Integer::sum);

        // Track routes accessed
        accumulator.routeCounts.merge(log.getHttpRoute(), 1, Integer::sum);

        return accumulator;
    }

    @Override
    public SessionMetrics getResult(SessionAccumulator accumulator) {
        return accumulator.toMetrics();
    }

    @Override
    public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
        if (a.requestCount == 0) return b;
        if (b.requestCount == 0) return a;

        SessionAccumulator merged = new SessionAccumulator();
        merged.clientIp = a.clientIp;
        merged.requestCount = a.requestCount + b.requestCount;
        merged.sessionStart = Math.min(a.sessionStart, b.sessionStart);
        merged.sessionEnd = Math.max(a.sessionEnd, b.sessionEnd);
        merged.totalResponseTimeMs = a.totalResponseTimeMs + b.totalResponseTimeMs;
        merged.totalBytesSent = a.totalBytesSent + b.totalBytesSent;
        merged.errorCount = a.errorCount + b.errorCount;

        merged.pathCounts.putAll(a.pathCounts);
        b.pathCounts.forEach((k, v) -> merged.pathCounts.merge(k, v, Integer::sum));

        merged.routeCounts.putAll(a.routeCounts);
        b.routeCounts.forEach((k, v) -> merged.routeCounts.merge(k, v, Integer::sum));

        return merged;
    }

    /**
     * Accumulator for session statistics.
     */
    public static class SessionAccumulator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        String clientIp;
        long requestCount;
        long sessionStart;
        long sessionEnd;
        long totalResponseTimeMs;
        long totalBytesSent;
        long errorCount;
        Map<String, Integer> pathCounts = new HashMap<>();
        Map<String, Integer> routeCounts = new HashMap<>();

        SessionMetrics toMetrics() {
            SessionMetrics metrics = new SessionMetrics();
            metrics.clientIp = clientIp;
            metrics.requestCount = requestCount;
            metrics.sessionStart = sessionStart;
            metrics.sessionEnd = sessionEnd;
            metrics.sessionDurationMs = sessionEnd - sessionStart;
            metrics.uniquePathCount = pathCounts.size();
            metrics.uniqueRouteCount = routeCounts.size();
            metrics.avgResponseTimeMs = requestCount > 0 ? 
                (double) totalResponseTimeMs / requestCount : 0;
            metrics.totalBytesSent = totalBytesSent;
            metrics.errorCount = errorCount;
            metrics.errorRate = requestCount > 0 ? 
                (double) errorCount / requestCount * 100 : 0;
            return metrics;
        }
    }

    /**
     * Output model for session metrics.
     */
    public static class SessionMetrics implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        public String clientIp;
        public long requestCount;
        public long sessionStart;
        public long sessionEnd;
        public long sessionDurationMs;
        public int uniquePathCount;
        public int uniqueRouteCount;
        public double avgResponseTimeMs;
        public long totalBytesSent;
        public long errorCount;
        public double errorRate;

        @Override
        public String toString() {
            return String.format(
                "Session[client=%s, requests=%d, duration=%dms, paths=%d, errors=%d]",
                clientIp, requestCount, sessionDurationMs, uniquePathCount, errorCount
            );
        }
    }
}
