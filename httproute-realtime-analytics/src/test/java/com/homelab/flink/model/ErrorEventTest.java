package com.homelab.flink.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ErrorEvent.
 */
class ErrorEventTest {

    @Test
    void shouldCreateFromAccessLog() {
        AccessLog log = AccessLog.builder()
            .timestamp("2026-01-21T12:00:00Z")
            .httpRoute("test-route")
            .hostname("app.example.com")
            .method("GET")
            .path("/api/test")
            .statusCode(500)
            .responseTimeMs(100)
            .upstreamCluster("test-cluster")
            .clientIp("10.0.0.1")
            .build();

        ErrorEvent event = ErrorEvent.fromAccessLog(log);

        assertThat(event.getHttpRoute()).isEqualTo("test-route");
        assertThat(event.getHostname()).isEqualTo("app.example.com");
        assertThat(event.getMethod()).isEqualTo("GET");
        assertThat(event.getPath()).isEqualTo("/api/test");
        assertThat(event.getStatusCode()).isEqualTo(500);
        assertThat(event.getErrorCategory()).isEqualTo("SERVER_ERROR");
        assertThat(event.getResponseTimeMs()).isEqualTo(100);
    }

    @Test
    void shouldClassifyClientError() {
        AccessLog log = AccessLog.builder()
            .httpRoute("test")
            .statusCode(404)
            .build();

        ErrorEvent event = ErrorEvent.fromAccessLog(log);

        assertThat(event.getErrorCategory()).isEqualTo("CLIENT_ERROR");
    }

    @Test
    void shouldClassifyServerError() {
        AccessLog log = AccessLog.builder()
            .httpRoute("test")
            .statusCode(503)
            .build();

        ErrorEvent event = ErrorEvent.fromAccessLog(log);

        assertThat(event.getErrorCategory()).isEqualTo("SERVER_ERROR");
    }
}
