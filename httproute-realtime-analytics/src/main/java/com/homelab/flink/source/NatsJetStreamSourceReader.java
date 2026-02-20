package com.homelab.flink.source;

import com.homelab.flink.common.NatsJetStreamSourceConfig;
import com.homelab.flink.model.AccessLog;

import io.nats.client.*;
import io.nats.client.api.*;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source reader for NATS JetStream source.
 * 
 * Responsible for reading records from assigned splits (NATS JetStream consumers).
 * This implements the Flink Source V2 API for exactly-once semantics.
 */
public class NatsJetStreamSourceReader implements SourceReader<AccessLog, NatsJetStreamSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSourceReader.class);

    private final SourceReaderContext context;
    private final NatsJetStreamSourceConfig config;
    private final AccessLogDeserializer deserializer;
    private final List<NatsJetStreamSplit> assignedSplits;
    
    private transient Connection natsConnection;
    private transient JetStream jetStream;
    private transient Map<String, JetStreamSubscription> subscriptions;
    private transient AtomicBoolean isRunning;
    private transient CompletableFuture<Void> availabilityFuture;

    public NatsJetStreamSourceReader(
            SourceReaderContext context,
            NatsJetStreamSourceConfig config) {
        this.context = context;
        this.config = config;
        this.deserializer = new AccessLogDeserializer();
        this.assignedSplits = new ArrayList<>();
        this.subscriptions = new HashMap<>();
    }

    @Override
    public void start() {
        LOG.info("Starting NATS JetStream source reader for subtask {}", 
            context.getIndexOfSubtask());
        
        isRunning = new AtomicBoolean(true);
        availabilityFuture = new CompletableFuture<>();
        
        ensureConnected();
    }

    /**
     * Lazily connect to NATS if not already connected.
     * 
     * Flink 2.0 may call addSplits() before start() during checkpoint recovery,
     * so connections must be established on-demand rather than only in start().
     */
    private synchronized void ensureConnected() {
        if (natsConnection != null && natsConnection.getStatus() == Connection.Status.CONNECTED) {
            return;
        }

        // Initialise lifecycle fields if start() has not run yet
        if (isRunning == null) {
            isRunning = new AtomicBoolean(true);
        }
        if (availabilityFuture == null) {
            availabilityFuture = new CompletableFuture<>();
        }

        try {
            connectToNats();
        } catch (Exception e) {
            LOG.error("Failed to connect to NATS", e);
            throw new RuntimeException("Failed to connect to NATS", e);
        }
    }

    private void connectToNats() throws IOException, InterruptedException {
        LOG.info("Connecting to NATS JetStream: {}", config.getNatsUrl());

        Options options = config.buildConnectionOptions()
            .connectionListener((conn, type) -> {
                LOG.info("NATS connection event: {}", type);
                if (type == ConnectionListener.Events.CONNECTED) {
                    availabilityFuture.complete(null);
                }
            })
            .errorListener(new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    LOG.error("NATS error: {}", error);
                }

                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    LOG.error("NATS exception", exp);
                }

                @Override
                public void slowConsumerDetected(Connection conn, Consumer consumer) {
                    LOG.warn("NATS slow consumer detected");
                }
            })
            .build();

        natsConnection = Nats.connect(options);
        jetStream = natsConnection.jetStream();
        
        LOG.info("Successfully connected to NATS");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<AccessLog> output) throws Exception {
        if (!isRunning.get()) {
            return InputStatus.END_OF_INPUT;
        }

        if (assignedSplits.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        boolean hasData = false;

        for (NatsJetStreamSplit split : assignedSplits) {
            JetStreamSubscription subscription = subscriptions.get(split.splitId());
            
            if (subscription == null) {
                continue;
            }

            try {
                // Fetch batch of messages with shorter timeout for better responsiveness
                Iterator<Message> messages = subscription.iterate(
                    config.getBatchSize(),
                    Duration.ofMillis(100)
                );

                while (messages.hasNext()) {
                    Message msg = messages.next();
                    hasData = true;
                    
                    try {
                        byte[] data = msg.getData();
                        AccessLog accessLog = deserializer.deserialize(data);
                        
                        if (accessLog != null) {
                            output.collect(accessLog, accessLog.getTimestampMillis());
                            split.setLastProcessedSequence(msg.metaData().streamSequence());
                        }
                        
                        msg.ack();
                        
                    } catch (Exception e) {
                        LOG.error("Error processing message: {}", 
                            new String(msg.getData(), StandardCharsets.UTF_8), e);
                        msg.nak();
                    }
                }
            } catch (IllegalStateException e) {
                LOG.warn("Subscription issue for split {}, will retry", split.splitId(), e);
            }
        }

        return hasData ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<NatsJetStreamSplit> snapshotState(long checkpointId) {
        LOG.debug("Snapshotting state at checkpoint {} with {} splits", 
            checkpointId, assignedSplits.size());
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<NatsJetStreamSplit> splits) {
        LOG.info("Adding {} splits to reader", splits.size());
        
        for (NatsJetStreamSplit split : splits) {
            assignedSplits.add(split);
            
            try {
                setupSubscription(split);
            } catch (Exception e) {
                LOG.error("Failed to setup subscription for split {}", split.splitId(), e);
                throw new RuntimeException("Failed to setup subscription", e);
            }
        }
        
        // Signal availability now that we have splits
        availabilityFuture.complete(null);
    }

    private void setupSubscription(NatsJetStreamSplit split) throws Exception {
        LOG.info("Setting up subscription for split {}", split.splitId());
        
        ensureConnected();
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        
        // Ensure stream exists
        try {
            jsm.getStreamInfo(config.getStream());
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == 10059) {
                LOG.info("Creating stream: {}", config.getStream());
                StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(config.getStream())
                    .subjects(config.getSubject())
                    .retentionPolicy(RetentionPolicy.Limits)
                    .maxAge(Duration.ofDays(7))
                    .storageType(StorageType.File)
                    .replicas(1)
                    .build();
                jsm.addStream(streamConfig);
            } else {
                throw e;
            }
        }

        // Build consumer configuration
        String consumerName = split.splitId();
        ConsumerConfiguration.Builder consumerConfigBuilder = ConsumerConfiguration.builder()
            .durable(consumerName)
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(config.getAckWait())
            .maxDeliver(5)
            .filterSubject(config.getSubject());

        // Set deliver policy based on restored sequence
        if (split.getLastProcessedSequence() > 0) {
            consumerConfigBuilder.deliverPolicy(DeliverPolicy.ByStartSequence);
            consumerConfigBuilder.startSequence(split.getLastProcessedSequence() + 1);
        } else {
            switch (config.getDeliverPolicy()) {
                case ALL:
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.All);
                    break;
                case LAST:
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.Last);
                    break;
                case NEW:
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.New);
                    break;
                case LAST_PER_SUBJECT:
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.LastPerSubject);
                    break;
                default:
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.All);
            }
        }

        ConsumerConfiguration consumerConfig = consumerConfigBuilder.build();

        try {
            jsm.addOrUpdateConsumer(config.getStream(), consumerConfig);
        } catch (JetStreamApiException e) {
            LOG.warn("Could not create consumer {}, it may already exist: {}", 
                consumerName, e.getMessage());
        }

        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
            .durable(consumerName)
            .stream(config.getStream())
            .build();

        JetStreamSubscription subscription = jetStream.subscribe(config.getSubject(), pullOptions);
        subscriptions.put(split.splitId(), subscription);
        
        LOG.info("Successfully subscribed to JetStream for split {}", split.splitId());
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.info("No more splits will be assigned to this reader");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing NATS JetStream source reader");
        
        if (isRunning != null) {
            isRunning.set(false);
        }
        
        if (subscriptions != null) {
            for (JetStreamSubscription subscription : subscriptions.values()) {
                try {
                    subscription.drain(Duration.ofSeconds(5));
                } catch (Exception e) {
                    LOG.warn("Error draining subscription", e);
                }
            }
        }
        
        if (natsConnection != null) {
            try {
                natsConnection.close();
            } catch (Exception e) {
                LOG.warn("Error closing NATS connection", e);
            }
        }
    }
}
