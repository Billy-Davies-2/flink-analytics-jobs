package com.homelab.flink.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
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

    // ConfigOptions for type-safe configuration
    public static final ConfigOption<String> NATS_URL = ConfigOptions
        .key("nats.url")
        .stringType()
        .defaultValue(DEFAULT_NATS_URL)
        .withDescription("NATS server URL");

    public static final ConfigOption<String> STREAM = ConfigOptions
        .key("nats.stream")
        .stringType()
        .defaultValue(DEFAULT_STREAM)
        .withDescription("JetStream stream name");

    public static final ConfigOption<String> SUBJECT = ConfigOptions
        .key("nats.subject")
        .stringType()
        .defaultValue(DEFAULT_SUBJECT)
        .withDescription("NATS subject to subscribe to");

    public static final ConfigOption<String> CONSUMER = ConfigOptions
        .key("nats.consumer")
        .stringType()
        .defaultValue(DEFAULT_CONSUMER)
        .withDescription("JetStream consumer name");

    public static final ConfigOption<String> DURABLE = ConfigOptions
        .key("nats.durable")
        .stringType()
        .defaultValue(DEFAULT_DURABLE)
        .withDescription("Durable consumer name");

    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions
        .key("nats.batch.size")
        .intType()
        .defaultValue(100)
        .withDescription("Batch size for fetching messages");

    public static final ConfigOption<Long> MAX_WAIT_MS = ConfigOptions
        .key("nats.max.wait.ms")
        .longType()
        .defaultValue(5000L)
        .withDescription("Maximum wait time in milliseconds");

    public static final ConfigOption<Long> ACK_WAIT_MS = ConfigOptions
        .key("nats.ack.wait.ms")
        .longType()
        .defaultValue(30000L)
        .withDescription("Acknowledgment wait time in milliseconds");

    public static final ConfigOption<String> DELIVER_POLICY = ConfigOptions
        .key("nats.deliver.policy")
        .stringType()
        .defaultValue("all")
        .withDescription("Deliver policy: all, last, new, last_per_subject");

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
     * Creates a NatsJetStreamSourceConfig from Configuration.
     *
     * @param config Flink Configuration
     */
    public NatsJetStreamSourceConfig(Configuration config) {
        this.natsUrl = config.get(NATS_URL);
        this.stream = config.get(STREAM);
        this.subject = config.get(SUBJECT);
        this.consumer = config.get(CONSUMER);
        this.durable = config.get(DURABLE);
        this.batchSize = config.get(BATCH_SIZE);
        this.maxWait = Duration.ofMillis(config.get(MAX_WAIT_MS));
        this.ackWait = Duration.ofMillis(config.get(ACK_WAIT_MS));
        this.deliverPolicy = parseDeliverPolicy(config.get(DELIVER_POLICY));
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
