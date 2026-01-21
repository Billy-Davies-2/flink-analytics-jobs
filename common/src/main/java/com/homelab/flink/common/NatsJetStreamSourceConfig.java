package com.homelab.flink.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Options;

import java.time.Duration;

/**
 * Configuration utility for setting up NATS JetStream sources in Flink jobs.
 * 
 * This class provides a fluent API for configuring NATS JetStream consumers with
 * sensible defaults for the homelab environment.
 */
public class NatsJetStreamSourceConfig {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSourceConfig.class);

    // Default configuration values
    public static final String DEFAULT_NATS_URL = "nats://nats.analytics.svc.cluster.local:4222";
    public static final String DEFAULT_STREAM = "ENVOY_ACCESS_LOGS";
    public static final String DEFAULT_SUBJECT = "envoy.access.logs";
    public static final String DEFAULT_CONSUMER = "flink-httproute-analytics";
    public static final String DEFAULT_DURABLE = "flink-httproute-analytics";

    // Parameter keys
    public static final String PARAM_NATS_URL = "nats.url";
    public static final String PARAM_STREAM = "nats.stream";
    public static final String PARAM_SUBJECT = "nats.subject";
    public static final String PARAM_CONSUMER = "nats.consumer";
    public static final String PARAM_DURABLE = "nats.durable";
    public static final String PARAM_BATCH_SIZE = "nats.batch.size";
    public static final String PARAM_MAX_WAIT_MS = "nats.max.wait.ms";
    public static final String PARAM_ACK_WAIT_MS = "nats.ack.wait.ms";
    public static final String PARAM_DELIVER_POLICY = "nats.deliver.policy";

    private final String natsUrl;
    private final String stream;
    private final String subject;
    private final String consumer;
    private final String durable;
    private final int batchSize;
    private final Duration maxWait;
    private final Duration ackWait;
    private final DeliverPolicy deliverPolicy;

    public enum DeliverPolicy {
        ALL,           // Deliver all messages
        LAST,          // Deliver last message only
        NEW,           // Deliver new messages only
        BY_START_TIME, // Deliver from a specific time
        LAST_PER_SUBJECT // Deliver last message per subject
    }

    /**
     * Creates a NatsJetStreamSourceConfig from ParameterTool arguments.
     *
     * @param params CLI parameters
     */
    public NatsJetStreamSourceConfig(ParameterTool params) {
        this.natsUrl = params.get(PARAM_NATS_URL, DEFAULT_NATS_URL);
        this.stream = params.get(PARAM_STREAM, DEFAULT_STREAM);
        this.subject = params.get(PARAM_SUBJECT, DEFAULT_SUBJECT);
        this.consumer = params.get(PARAM_CONSUMER, DEFAULT_CONSUMER);
        this.durable = params.get(PARAM_DURABLE, DEFAULT_DURABLE);
        this.batchSize = params.getInt(PARAM_BATCH_SIZE, 100);
        this.maxWait = Duration.ofMillis(params.getLong(PARAM_MAX_WAIT_MS, 5000));
        this.ackWait = Duration.ofMillis(params.getLong(PARAM_ACK_WAIT_MS, 30000));
        this.deliverPolicy = parseDeliverPolicy(params.get(PARAM_DELIVER_POLICY, "all"));
    }

    /**
     * Creates a NatsJetStreamSourceConfig with explicit values.
     */
    public NatsJetStreamSourceConfig(String natsUrl, String stream, String subject, String consumer) {
        this.natsUrl = natsUrl;
        this.stream = stream;
        this.subject = subject;
        this.consumer = consumer;
        this.durable = consumer;
        this.batchSize = 100;
        this.maxWait = Duration.ofSeconds(5);
        this.ackWait = Duration.ofSeconds(30);
        this.deliverPolicy = DeliverPolicy.ALL;
    }

    /**
     * Creates a NatsJetStreamSourceConfig with default values.
     */
    public static NatsJetStreamSourceConfig defaults() {
        return new NatsJetStreamSourceConfig(DEFAULT_NATS_URL, DEFAULT_STREAM, DEFAULT_SUBJECT, DEFAULT_CONSUMER);
    }

    /**
     * Parses the deliver policy from a string value.
     */
    private DeliverPolicy parseDeliverPolicy(String policy) {
        switch (policy.toLowerCase()) {
            case "all":
                return DeliverPolicy.ALL;
            case "last":
                return DeliverPolicy.LAST;
            case "new":
                return DeliverPolicy.NEW;
            case "last_per_subject":
                return DeliverPolicy.LAST_PER_SUBJECT;
            default:
                LOG.warn("Unknown deliver policy '{}', using ALL", policy);
                return DeliverPolicy.ALL;
        }
    }

    /**
     * Builds NATS connection options.
     *
     * @return NATS Options builder
     */
    public Options.Builder buildConnectionOptions() {
        return new Options.Builder()
            .server(natsUrl)
            .connectionName("flink-" + consumer)
            .maxReconnects(-1)  // Unlimited reconnects
            .reconnectWait(Duration.ofSeconds(2))
            .connectionTimeout(Duration.ofSeconds(10))
            .pingInterval(Duration.ofSeconds(30));
    }

    // Getters
    public String getNatsUrl() {
        return natsUrl;
    }

    public String getStream() {
        return stream;
    }

    public String getSubject() {
        return subject;
    }

    public String getConsumer() {
        return consumer;
    }

    public String getDurable() {
        return durable;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Duration getMaxWait() {
        return maxWait;
    }

    public Duration getAckWait() {
        return ackWait;
    }

    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    @Override
    public String toString() {
        return String.format(
            "NatsJetStreamSourceConfig[url=%s, stream=%s, subject=%s, consumer=%s]",
            natsUrl, stream, subject, consumer
        );
    }
}
