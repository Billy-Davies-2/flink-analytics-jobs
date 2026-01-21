-- =============================================================================
-- NATS JetStream Source for Access Logs
-- =============================================================================
-- Creates a source that consumes from NATS JetStream topic
--
-- Prerequisites:
--   - NATS JetStream running with stream ENVOY_ACCESS_LOGS
--   - Subject: envoy.access.logs
-- =============================================================================

CREATE SOURCE IF NOT EXISTS access_logs (
    timestamp TIMESTAMPTZ,
    http_route VARCHAR,
    hostname VARCHAR,
    method VARCHAR,
    path VARCHAR,
    status_code INT,
    response_time_ms BIGINT,
    bytes_sent BIGINT,
    bytes_received BIGINT,
    upstream_cluster VARCHAR,
    client_ip VARCHAR
) WITH (
    connector = 'nats',
    server_url = 'nats://nats:4222',
    subject = 'envoy.access.logs',
    stream = 'ENVOY_ACCESS_LOGS',
    consumer.durable_name = 'risingwave-analytics',
    consumer.ack_policy = 'explicit'
) FORMAT PLAIN ENCODE JSON;

-- Verify source was created
-- SELECT * FROM rw_catalog.rw_sources WHERE name = 'access_logs';
