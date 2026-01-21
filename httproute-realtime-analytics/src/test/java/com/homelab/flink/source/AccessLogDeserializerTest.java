package com.homelab.flink.source;

import com.homelab.flink.model.AccessLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for AccessLogDeserializer.
 */
class AccessLogDeserializerTest {

    private AccessLogDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new AccessLogDeserializer();
    }

    @Test
    void shouldDeserializeValidAccessLog() throws IOException {
        String json = """
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
            """;

        AccessLog log = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(log).isNotNull();
        assertThat(log.getHttpRoute()).isEqualTo("productpage");
        assertThat(log.getHostname()).isEqualTo("app.example.com");
        assertThat(log.getMethod()).isEqualTo("GET");
        assertThat(log.getPath()).isEqualTo("/api/v1/products");
        assertThat(log.getStatusCode()).isEqualTo(200);
        assertThat(log.getResponseTimeMs()).isEqualTo(45);
        assertThat(log.getBytesSent()).isEqualTo(1024);
        assertThat(log.getBytesReceived()).isEqualTo(256);
        assertThat(log.getUpstreamCluster()).isEqualTo("productpage-cluster");
        assertThat(log.getClientIp()).isEqualTo("10.42.1.100");
    }

    @Test
    void shouldParseTimestampCorrectly() throws IOException {
        String json = """
            {
                "timestamp": "2026-01-21T12:00:00Z",
                "http_route": "test",
                "status_code": 200
            }
            """;

        AccessLog log = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(log.getTimestampMillis()).isEqualTo(1768996800000L);
    }

    @Test
    void shouldHandleMissingHttpRoute() throws IOException {
        String json = """
            {
                "timestamp": "2026-01-21T12:00:00Z",
                "status_code": 200
            }
            """;

        AccessLog log = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(log).isNotNull();
        assertThat(log.getHttpRoute()).isEqualTo("unknown");
    }

    @Test
    void shouldHandleUnknownFields() throws IOException {
        String json = """
            {
                "timestamp": "2026-01-21T12:00:00Z",
                "http_route": "test",
                "status_code": 200,
                "unknown_field": "should be ignored"
            }
            """;

        AccessLog log = deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(log).isNotNull();
        assertThat(log.getHttpRoute()).isEqualTo("test");
    }

    @Test
    void shouldReturnNullForNullMessage() throws IOException {
        AccessLog log = deserializer.deserialize(null);
        assertThat(log).isNull();
    }

    @Test
    void shouldReturnNullForEmptyMessage() throws IOException {
        AccessLog log = deserializer.deserialize(new byte[0]);
        assertThat(log).isNull();
    }

    @Test
    void shouldReturnNullForInvalidJson() throws IOException {
        String invalidJson = "not valid json";
        AccessLog log = deserializer.deserialize(invalidJson.getBytes(StandardCharsets.UTF_8));
        assertThat(log).isNull();
    }

    @Test
    void shouldIdentifyErrorResponses() throws IOException {
        String json404 = """
            {"http_route": "test", "status_code": 404}
            """;
        String json500 = """
            {"http_route": "test", "status_code": 500}
            """;
        String json200 = """
            {"http_route": "test", "status_code": 200}
            """;

        AccessLog log404 = deserializer.deserialize(json404.getBytes(StandardCharsets.UTF_8));
        AccessLog log500 = deserializer.deserialize(json500.getBytes(StandardCharsets.UTF_8));
        AccessLog log200 = deserializer.deserialize(json200.getBytes(StandardCharsets.UTF_8));

        assertThat(log404.isError()).isTrue();
        assertThat(log404.isClientError()).isTrue();
        assertThat(log404.isServerError()).isFalse();

        assertThat(log500.isError()).isTrue();
        assertThat(log500.isClientError()).isFalse();
        assertThat(log500.isServerError()).isTrue();

        assertThat(log200.isError()).isFalse();
    }

    @Test
    void shouldReturnCorrectStatusCategory() throws IOException {
        String json200 = """
            {"http_route": "test", "status_code": 201}
            """;
        String json404 = """
            {"http_route": "test", "status_code": 404}
            """;
        String json503 = """
            {"http_route": "test", "status_code": 503}
            """;

        AccessLog log200 = deserializer.deserialize(json200.getBytes(StandardCharsets.UTF_8));
        AccessLog log404 = deserializer.deserialize(json404.getBytes(StandardCharsets.UTF_8));
        AccessLog log503 = deserializer.deserialize(json503.getBytes(StandardCharsets.UTF_8));

        assertThat(log200.getStatusCategory()).isEqualTo("2xx");
        assertThat(log404.getStatusCategory()).isEqualTo("4xx");
        assertThat(log503.getStatusCategory()).isEqualTo("5xx");
    }
}
