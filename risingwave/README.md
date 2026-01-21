# RisingWave Alternative

RisingWave is a streaming database that provides a simpler SQL-based alternative to Flink for stream processing. It can:

- Consume directly from NATS JetStream
- Create materialized views for real-time analytics
- Write to Iceberg tables (via Iceberg sink)
- Expose PostgreSQL-compatible queries

## Connecting to RisingWave

```bash
# Connect using psql
psql -h localhost -p 4566 -d dev -U root

# Or use the RisingWave CLI
docker exec -it flink-analytics-jobs_risingwave_1 psql -h localhost -p 4566 -d dev -U root
```

## Setup

Run the SQL scripts in order:

1. `01-sources.sql` - Create NATS JetStream source
2. `02-materialized-views.sql` - Create materialized views for analytics
3. `03-iceberg-sinks.sql` - Create Iceberg sinks for persistence

```bash
# Run all setup scripts
for f in risingwave/*.sql; do
    psql -h localhost -p 4566 -d dev -U root -f "$f"
done
```

## Architecture Comparison

| Feature | Flink | RisingWave |
|---------|-------|------------|
| Language | Java/Scala | SQL |
| State Management | RocksDB/Memory | Built-in |
| Checkpointing | Explicit configuration | Automatic |
| Learning Curve | Steeper | Simpler |
| Flexibility | More control | SQL-limited |
| Deployment | JAR-based | Database server |

## Querying Materialized Views

```sql
-- Real-time route metrics (updated continuously)
SELECT * FROM route_metrics_1m ORDER BY window_end DESC LIMIT 10;

-- Error events stream
SELECT * FROM error_events ORDER BY timestamp DESC LIMIT 20;

-- Latency alerts
SELECT * FROM latency_alerts WHERE alerted_at > NOW() - INTERVAL '1 hour';
```

## Monitoring

RisingWave exposes Prometheus metrics at `http://localhost:5691/metrics`.

```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: 'risingwave'
    static_configs:
      - targets: ['risingwave:5691']
```
