package com.homelab.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * POJO representing an Envoy Gateway access log event.
 * 
 * This model captures HTTP request/response metadata from Envoy Gateway,
 * including timing, routing, and response information needed for analytics.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Timestamp when the request was received (ISO-8601 format).
     */
    @JsonProperty("timestamp")
    private String timestamp;

    /**
     * The HTTPRoute name that matched the request.
     */
    @JsonProperty("http_route")
    private String httpRoute;

    /**
     * The hostname from the Host header.
     */
    @JsonProperty("hostname")
    private String hostname;

    /**
     * HTTP method (GET, POST, PUT, DELETE, etc.).
     */
    @JsonProperty("method")
    private String method;

    /**
     * Request path including query string.
     */
    @JsonProperty("path")
    private String path;

    /**
     * HTTP response status code.
     */
    @JsonProperty("status_code")
    private int statusCode;

    /**
     * Total response time in milliseconds.
     */
    @JsonProperty("response_time_ms")
    private long responseTimeMs;

    /**
     * Number of bytes sent in the response body.
     */
    @JsonProperty("bytes_sent")
    private long bytesSent;

    /**
     * Number of bytes received in the request body.
     */
    @JsonProperty("bytes_received")
    private long bytesReceived;

    /**
     * The upstream cluster that handled the request.
     */
    @JsonProperty("upstream_cluster")
    private String upstreamCluster;

    /**
     * Client IP address.
     */
    @JsonProperty("client_ip")
    private String clientIp;

    /**
     * Returns the timestamp as epoch milliseconds for event-time processing.
     */
    public long getTimestampMillis() {
        if (timestamp == null || timestamp.isEmpty()) {
            return System.currentTimeMillis();
        }
        try {
            return Instant.parse(timestamp).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    /**
     * Returns the timestamp as an Instant.
     */
    public Instant getTimestampAsInstant() {
        if (timestamp == null || timestamp.isEmpty()) {
            return Instant.now();
        }
        try {
            return Instant.parse(timestamp);
        } catch (Exception e) {
            return Instant.now();
        }
    }

    /**
     * Checks if this is an error response (4xx or 5xx).
     */
    public boolean isError() {
        return statusCode >= 400;
    }

    /**
     * Checks if this is a client error (4xx).
     */
    public boolean isClientError() {
        return statusCode >= 400 && statusCode < 500;
    }

    /**
     * Checks if this is a server error (5xx).
     */
    public boolean isServerError() {
        return statusCode >= 500;
    }

    /**
     * Returns the status code category (2xx, 3xx, 4xx, 5xx).
     */
    public String getStatusCategory() {
        int category = statusCode / 100;
        return category + "xx";
    }
}
