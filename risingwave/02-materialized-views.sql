-- =============================================================================
-- Materialized Views for HTTPRoute Analytics
-- =============================================================================
-- These views are continuously updated as new data arrives from NATS JetStream
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1-Minute Tumbling Window Aggregations
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS route_metrics_1m AS
SELECT 
    http_route,
    window_start,
    window_end,
    COUNT(*) AS request_count,
    COUNT(*) FILTER (WHERE status_code >= 200 AND status_code < 300) AS success_count,
    COUNT(*) FILTER (WHERE status_code >= 400 AND status_code < 500) AS client_error_count,
    COUNT(*) FILTER (WHERE status_code >= 500) AS server_error_count,
    AVG(response_time_ms)::FLOAT AS avg_latency_ms,
    MIN(response_time_ms) AS min_latency_ms,
    MAX(response_time_ms) AS max_latency_ms,
    SUM(bytes_sent) AS total_bytes_sent,
    SUM(bytes_received) AS total_bytes_received,
    -- Calculate error rate
    (COUNT(*) FILTER (WHERE status_code >= 400)::FLOAT / NULLIF(COUNT(*), 0) * 100) AS error_rate_pct
FROM TUMBLE(access_logs, timestamp, INTERVAL '1 MINUTE')
GROUP BY http_route, window_start, window_end;

-- -----------------------------------------------------------------------------
-- 5-Minute Sliding Window with Percentiles
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS route_metrics_5m AS
SELECT 
    http_route,
    window_start,
    window_end,
    COUNT(*) AS request_count,
    AVG(response_time_ms)::FLOAT AS avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time_ms) AS p50_latency_ms,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY response_time_ms) AS p90_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) AS p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) AS p99_latency_ms,
    (COUNT(*) FILTER (WHERE status_code >= 400)::FLOAT / NULLIF(COUNT(*), 0) * 100) AS error_rate_pct,
    -- Requests per second in this window
    (COUNT(*)::FLOAT / 300) AS requests_per_second
FROM HOP(access_logs, timestamp, INTERVAL '1 MINUTE', INTERVAL '5 MINUTES')
GROUP BY http_route, window_start, window_end;

-- -----------------------------------------------------------------------------
-- Error Events Stream (individual errors)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS error_events AS
SELECT 
    timestamp,
    http_route,
    hostname,
    method,
    path,
    status_code,
    response_time_ms,
    client_ip,
    upstream_cluster,
    CASE 
        WHEN status_code >= 500 THEN 'server_error'
        WHEN status_code >= 400 THEN 'client_error'
        ELSE 'unknown'
    END AS error_type
FROM access_logs
WHERE status_code >= 400;

-- -----------------------------------------------------------------------------
-- Latency Alerts (P99 > threshold)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS latency_alerts AS
SELECT 
    http_route,
    window_start,
    window_end,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) AS p99_latency_ms,
    COUNT(*) AS sample_count,
    window_end AS alerted_at
FROM TUMBLE(access_logs, timestamp, INTERVAL '1 MINUTE')
GROUP BY http_route, window_start, window_end
HAVING PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) > 500;

-- -----------------------------------------------------------------------------
-- Route Summary (current state)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS route_summary AS
SELECT 
    http_route,
    COUNT(*) AS total_requests,
    COUNT(*) FILTER (WHERE status_code >= 200 AND status_code < 300) AS total_success,
    COUNT(*) FILTER (WHERE status_code >= 400) AS total_errors,
    AVG(response_time_ms)::FLOAT AS overall_avg_latency_ms,
    MAX(timestamp) AS last_seen
FROM access_logs
GROUP BY http_route;

-- -----------------------------------------------------------------------------
-- Hostname Traffic Distribution
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS hostname_traffic AS
SELECT 
    hostname,
    window_start,
    window_end,
    COUNT(*) AS request_count,
    COUNT(DISTINCT http_route) AS unique_routes,
    AVG(response_time_ms)::FLOAT AS avg_latency_ms
FROM TUMBLE(access_logs, timestamp, INTERVAL '5 MINUTES')
GROUP BY hostname, window_start, window_end;

-- -----------------------------------------------------------------------------
-- Client IP Analysis (for rate limiting insights)
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS client_activity AS
SELECT 
    client_ip,
    window_start,
    window_end,
    COUNT(*) AS request_count,
    COUNT(DISTINCT http_route) AS routes_accessed,
    COUNT(*) FILTER (WHERE status_code >= 400) AS error_count
FROM TUMBLE(access_logs, timestamp, INTERVAL '1 MINUTE')
GROUP BY client_ip, window_start, window_end
HAVING COUNT(*) > 10;  -- Only track active clients
