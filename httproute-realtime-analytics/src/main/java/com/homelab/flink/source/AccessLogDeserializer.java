package com.homelab.flink.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.homelab.flink.model.AccessLog;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Kafka deserializer for Envoy Gateway access log JSON messages.
 * 
 * This deserializer converts JSON-encoded access log events from Kafka
 * into AccessLog POJOs for processing in the Flink pipeline.
 */
public class AccessLogDeserializer implements DeserializationSchema<AccessLog> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AccessLogDeserializer.class);

    private transient ObjectMapper objectMapper;

    /**
     * Initializes the ObjectMapper on first use (lazy initialization).
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        }
        return objectMapper;
    }

    @Override
    public AccessLog deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            LOG.warn("Received null or empty message, skipping");
            return null;
        }

        try {
            AccessLog log = getObjectMapper().readValue(message, AccessLog.class);
            
            // Validate required fields
            if (log.getHttpRoute() == null || log.getHttpRoute().isEmpty()) {
                LOG.warn("Access log missing http_route field, using 'unknown': {}", 
                    new String(message, StandardCharsets.UTF_8));
                log.setHttpRoute("unknown");
            }

            return log;
        } catch (Exception e) {
            LOG.error("Failed to deserialize access log: {}", 
                new String(message, StandardCharsets.UTF_8), e);
            // Return null to skip this message rather than fail the job
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(AccessLog nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AccessLog> getProducedType() {
        return TypeInformation.of(AccessLog.class);
    }
}
