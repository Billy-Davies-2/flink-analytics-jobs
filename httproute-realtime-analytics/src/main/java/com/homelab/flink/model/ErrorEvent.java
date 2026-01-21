package com.homelab.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * POJO representing an individual error event (4xx/5xx response).
 * 
 * This model captures detailed information about error responses for
 * troubleshooting and error pattern analysis.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Event timestamp in epoch milliseconds.
     */
    private long eventTime;

    /**
     * The HTTPRoute name.
     */
    private String httpRoute;

    /**
     * The hostname from the Host header.
     */
    private String hostname;

    /**
     * HTTP method.
     */
    private String method;

    /**
     * Request path.
     */
    private String path;

    /**
     * HTTP response status code.
     */
    private int statusCode;

    /**
     * Error category (CLIENT_ERROR or SERVER_ERROR).
     */
    private String errorCategory;

    /**
     * Response time in milliseconds.
     */
    private long responseTimeMs;

    /**
     * The upstream cluster that handled the request.
     */
    private String upstreamCluster;

    /**
     * Client IP address.
     */
    private String clientIp;

    /**
     * Processing timestamp when this event was captured.
     */
    private long processingTime;

    /**
     * Creates an ErrorEvent from an AccessLog.
     */
    public static ErrorEvent fromAccessLog(AccessLog log) {
        return ErrorEvent.builder()
            .eventTime(log.getTimestampMillis())
            .httpRoute(log.getHttpRoute())
            .hostname(log.getHostname())
            .method(log.getMethod())
            .path(log.getPath())
            .statusCode(log.getStatusCode())
            .errorCategory(log.isServerError() ? "SERVER_ERROR" : "CLIENT_ERROR")
            .responseTimeMs(log.getResponseTimeMs())
            .upstreamCluster(log.getUpstreamCluster())
            .clientIp(log.getClientIp())
            .processingTime(System.currentTimeMillis())
            .build();
    }
}
