#!/bin/bash
#
# Generate sample access log events and publish to NATS JetStream
#
# Usage: ./generate-sample-data.sh [count] [subject] [nats-url]
#
# Requirements: NATS CLI (nats) must be installed
#   macOS: brew install nats-io/nats-tools/nats
#   Linux: Download from https://github.com/nats-io/natscli/releases
#

COUNT=${1:-100}
SUBJECT=${2:-envoy.access.logs}
NATS_URL=${3:-nats://localhost:4222}

ROUTES=("productpage" "checkout" "cart" "reviews" "ratings" "details")
METHODS=("GET" "POST" "PUT" "DELETE")
HOSTNAMES=("app.example.com" "api.example.com" "shop.example.com")
STATUS_CODES=(200 200 200 200 200 201 204 301 400 401 403 404 500 502 503)

# Check if nats CLI is available
if ! command -v nats &> /dev/null; then
    echo "ERROR: NATS CLI not found. Please install it:"
    echo "  macOS: brew install nats-io/nats-tools/nats"
    echo "  Linux: Download from https://github.com/nats-io/natscli/releases"
    exit 1
fi

echo "Generating $COUNT access log events to subject $SUBJECT..."
echo "NATS URL: $NATS_URL"

for i in $(seq 1 $COUNT); do
    ROUTE=${ROUTES[$RANDOM % ${#ROUTES[@]}]}
    METHOD=${METHODS[$RANDOM % ${#METHODS[@]}]}
    HOSTNAME=${HOSTNAMES[$RANDOM % ${#HOSTNAMES[@]}]}
    STATUS=${STATUS_CODES[$RANDOM % ${#STATUS_CODES[@]}]}
    RESPONSE_TIME=$((RANDOM % 500 + 10))
    BYTES_SENT=$((RANDOM % 10000 + 100))
    BYTES_RECEIVED=$((RANDOM % 5000 + 50))
    CLIENT_IP="10.42.$((RANDOM % 10)).$((RANDOM % 255))"
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    JSON=$(cat <<EOF
{"timestamp":"$TIMESTAMP","http_route":"$ROUTE","hostname":"$HOSTNAME","method":"$METHOD","path":"/api/v1/$ROUTE","status_code":$STATUS,"response_time_ms":$RESPONSE_TIME,"bytes_sent":$BYTES_SENT,"bytes_received":$BYTES_RECEIVED,"upstream_cluster":"${ROUTE}-cluster","client_ip":"$CLIENT_IP"}
EOF
)

    # Publish to NATS JetStream
    echo "$JSON" | nats pub "$SUBJECT" --server "$NATS_URL" 2>/dev/null

    if [ $((i % 10)) -eq 0 ]; then
        echo "Sent $i/$COUNT events..."
    fi

    # Small delay to simulate realistic traffic
    sleep 0.1
done

echo "Done! Published $COUNT events to $SUBJECT"
