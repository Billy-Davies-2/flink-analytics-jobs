#!/bin/bash
#
# Submit the Flink job to the local cluster
#
# Usage: ./submit-job.sh [jar-path] [job-args...]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

JAR_PATH="${1:-$PROJECT_ROOT/httproute-realtime-analytics/target/httproute-realtime-analytics-1.0.0-SNAPSHOT-shaded.jar}"
shift 2>/dev/null || true

FLINK_JOBMANAGER="${FLINK_JOBMANAGER:-http://localhost:8081}"

# Default arguments for local development
DEFAULT_ARGS=(
    "--nats.url" "nats://localhost:4222"
    "--nats.stream" "ENVOY_ACCESS_LOGS"
    "--nats.subject" "envoy.access.logs"
    "--nats.consumer" "flink-httproute-analytics"
    "--nats.durable" "flink-httproute-analytics"
    "--nessie.uri" "http://localhost:19120/api/v1"
    "--iceberg.warehouse" "s3a://iceberg-warehouse"
    "--s3.endpoint" "http://localhost:9000"
    "--s3.access.key" "minioadmin"
    "--s3.secret.key" "minioadmin"
    "--s3.path.style.access" "true"
    "--parallelism" "2"
    "--latency.threshold.ms" "500"
)

# Use provided args or defaults
if [ $# -gt 0 ]; then
    ARGS=("$@")
else
    ARGS=("${DEFAULT_ARGS[@]}")
fi

echo "Submitting job from: $JAR_PATH"
echo "Flink JobManager: $FLINK_JOBMANAGER"
echo "Arguments: ${ARGS[*]}"

# Check if jar exists
if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found: $JAR_PATH"
    echo "Please build the project first: mvn clean package -DskipTests"
    exit 1
fi

# Submit via REST API
UPLOAD_RESPONSE=$(curl -s -X POST \
    -H "Expect:" \
    -F "jarfile=@$JAR_PATH" \
    "$FLINK_JOBMANAGER/jars/upload")

JAR_ID=$(echo "$UPLOAD_RESPONSE" | jq -r '.filename' | sed 's/.*\///')

if [ -z "$JAR_ID" ] || [ "$JAR_ID" = "null" ]; then
    echo "ERROR: Failed to upload JAR"
    echo "$UPLOAD_RESPONSE"
    exit 1
fi

echo "Uploaded JAR: $JAR_ID"

# Build program args string
PROGRAM_ARGS=$(printf '%s ' "${ARGS[@]}")

# Run the job
RUN_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"programArgs\": \"$PROGRAM_ARGS\"}" \
    "$FLINK_JOBMANAGER/jars/$JAR_ID/run")

JOB_ID=$(echo "$RUN_RESPONSE" | jq -r '.jobid')

if [ -z "$JOB_ID" ] || [ "$JOB_ID" = "null" ]; then
    echo "ERROR: Failed to start job"
    echo "$RUN_RESPONSE"
    exit 1
fi

echo "Job started successfully!"
echo "Job ID: $JOB_ID"
echo "Dashboard: $FLINK_JOBMANAGER/#/job/$JOB_ID"
