# Flink Analytics Jobs

Real-time analytics processing for Kubernetes homelab using Apache Flink, writing to Apache Iceberg tables via Nessie catalog.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Envoy Gateway  │───▶│ NATS JetStream   │───▶│   Flink Job     │───▶│  Iceberg/Nessie │
│  Access Logs    │    │                  │    │  (This Repo)    │    │     Tables      │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
                                                                              │
                                                                              ▼
                                                                       ┌─────────────┐
                                                                       │    Trino    │
                                                                       │   Queries   │
                                                                       └─────────────┘
```

### Alternative: RisingWave

For simpler SQL-based streaming analytics, consider using [RisingWave](https://risingwave.com/) which can:
- Consume directly from NATS JetStream
- Write to Iceberg tables
- Provide materialized views for real-time queries

See [risingwave/README.md](risingwave/README.md) for RisingWave-based analytics queries.

## Jobs

### HTTPRoute Real-Time Analytics

Processes Envoy Gateway access logs from NATS JetStream and produces:

| Table | Description | Window |
|-------|-------------|--------|
| `httproute_analytics.route_metrics_1m` | Per-minute aggregations per route | 1-min tumbling |
| `httproute_analytics.route_metrics_5m` | Rolling aggregations with trends | 5-min sliding |
| `httproute_analytics.error_events` | Individual 4xx/5xx error events | None (streaming) |
| `httproute_analytics.latency_alerts` | P99 latency threshold breaches | 1-min tumbling |

## Prerequisites

- Flink Operator deployed in `analytics` namespace
- NATS with JetStream enabled at `nats://nats.analytics.svc.cluster.local:4222`
- Apache Nessie catalog at `http://nessie.analytics.svc.cluster.local:19120/api/v1`
- S3-compatible storage for Iceberg warehouse

## Building

```bash
# Build all modules
mvn clean package

# Build with tests
mvn clean verify

# Build specific module
mvn clean package -pl httproute-realtime-analytics -am
```

## Configuration

### CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--nats.url` | `nats://nats.analytics.svc.cluster.local:4222` | NATS server URL |
| `--nats.stream` | `ENVOY_ACCESS_LOGS` | JetStream stream name |
| `--nats.subject` | `envoy.access.logs` | Subject to subscribe to |
| `--nats.consumer` | `flink-httproute-analytics` | Consumer name |
| `--nats.durable` | `flink-httproute-analytics` | Durable consumer name |
| `--nats.deliver.policy` | `all` | Deliver policy (all, last, new) |
| `--nessie.uri` | `http://nessie.analytics.svc.cluster.local:19120/api/v1` | Nessie catalog URI |
| `--iceberg.warehouse` | `s3a://iceberg-warehouse` | Iceberg warehouse path |
| `--parallelism` | `2` | Job parallelism |
| `--latency.threshold.ms` | `500` | P99 latency alert threshold |

## NATS JetStream Setup

Create the stream for access logs:

```bash
# Using NATS CLI
nats stream add ENVOY_ACCESS_LOGS \
  --subjects "envoy.access.logs" \
  --retention limits \
  --max-age 7d \
  --storage file \
  --replicas 1

# Create a consumer for Flink
nats consumer add ENVOY_ACCESS_LOGS flink-httproute-analytics \
  --ack explicit \
  --deliver all \
  --max-deliver 5 \
  --filter "envoy.access.logs"
```

## Deployment

### FlinkDeployment CRD

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: httproute-realtime-analytics
  namespace: analytics
spec:
  image: flink:1.20.0-java21
  flinkVersion: v1_20
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.backend: rocksdb
    state.checkpoints.dir: s3a://flink-checkpoints/httproute-analytics
    state.savepoints.dir: s3a://flink-savepoints/httproute-analytics
  serviceAccount: flink
  jobManager:
    resource:
      memory: "1024m"
      cpu: 0.5
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
    replicas: 2
  job:
    jarURI: local:///opt/flink/usrlib/httproute-realtime-analytics.jar
    args:
      - "--nats.url"
      - "nats://nats.analytics.svc.cluster.local:4222"
      - "--nats.stream"
      - "ENVOY_ACCESS_LOGS"
      - "--nessie.uri"
      - "http://nessie.analytics.svc.cluster.local:19120/api/v1"
    parallelism: 2
    upgradeMode: savepoint
    state: running
```

## Input Schema

Access logs from Envoy Gateway (published to NATS JetStream):

```json
{
  "timestamp": "2026-01-21T12:00:00Z",
  "http_route": "productpage",
  "hostname": "app.example.com",
  "method": "GET",
  "path": "/api/v1/products",
  "status_code": 200,
  "response_time_ms": 45,
  "bytes_sent": 1024,
  "bytes_received": 256,
  "upstream_cluster": "productpage-cluster",
  "client_ip": "10.42.1.100"
}
```

## Querying with Trino

```sql
-- Request rate per route (last hour)
SELECT 
    http_route,
    SUM(request_count) as total_requests,
    AVG(avg_latency_ms) as avg_latency,
    MAX(p99_latency_ms) as max_p99
FROM httproute_analytics.route_metrics_1m
WHERE window_end > NOW() - INTERVAL '1' HOUR
GROUP BY http_route
ORDER BY total_requests DESC;

-- Error rate trend
SELECT 
    http_route,
    window_end,
    error_rate_pct
FROM httproute_analytics.route_metrics_5m
WHERE error_rate_pct > 5
ORDER BY window_end DESC;
```

## RisingWave Alternative

If you prefer SQL-based streaming analytics, RisingWave can provide similar functionality with simpler setup:

```sql
-- Create NATS source in RisingWave
CREATE SOURCE access_logs (
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
    server_url = 'nats://nats.analytics.svc.cluster.local:4222',
    subject = 'envoy.access.logs',
    stream = 'ENVOY_ACCESS_LOGS',
    consumer.durable_name = 'risingwave-analytics'
) FORMAT PLAIN ENCODE JSON;

-- Create materialized view for 1-minute aggregations
CREATE MATERIALIZED VIEW route_metrics_1m AS
SELECT 
    http_route,
    window_start,
    window_end,
    COUNT(*) as request_count,
    COUNT(*) FILTER (WHERE status_code >= 200 AND status_code < 300) as success_count,
    COUNT(*) FILTER (WHERE status_code >= 400 AND status_code < 500) as client_error_count,
    COUNT(*) FILTER (WHERE status_code >= 500) as server_error_count,
    AVG(response_time_ms) as avg_latency_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time_ms) as p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time_ms) as p99_latency_ms
FROM TUMBLE(access_logs, timestamp, INTERVAL '1' MINUTE)
GROUP BY http_route, window_start, window_end;
```

## Development

### Running Locally

```bash
# Start local NATS and Nessie (using Docker Compose)
docker-compose up -d

# Run the job
mvn exec:java -pl httproute-realtime-analytics \
  -Dexec.mainClass="com.homelab.flink.HTTPRouteRealtimeAnalytics" \
  -Dexec.args="--nats.url nats://localhost:4222 --nessie.uri http://localhost:19120/api/v1"
```

### Testing

```bash
# Run all tests
mvn test

# Run integration tests
mvn verify -P integration-tests
```

## Releases

Releases are automated via GitHub Actions. To create a release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

This will:
1. Build all modules
2. Create a GitHub Release
3. Upload JAR files as release assets

## License

MIT License
