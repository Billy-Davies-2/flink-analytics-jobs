package com.homelab.flink.source;

import com.homelab.flink.common.NatsJetStreamSourceConfig;
import com.homelab.flink.model.AccessLog;

import io.nats.client.*;
import io.nats.client.api.*;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NATS JetStream source for Flink.
 * 
 * This source consumes messages from a NATS JetStream stream and emits them
 * as AccessLog objects for processing in the Flink pipeline.
 * 
 * Features:
 * - Durable consumer support for exactly-once semantics
 * - Automatic reconnection handling
 * - Checkpoint-based acknowledgment
 * - Parallel consumption support
 */
public class NatsJetStreamSource extends RichParallelSourceFunction<AccessLog> 
    implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSource.class);

    private final NatsJetStreamSourceConfig config;
    private final AccessLogDeserializer deserializer;

    private transient Connection natsConnection;
    private transient JetStream jetStream;
    private transient JetStreamSubscription subscription;
    private transient AtomicBoolean running;
    
    // For checkpoint-based acknowledgment
    private transient ListState<Long> checkpointedSequences;
    private transient volatile long lastProcessedSequence;

    public NatsJetStreamSource(NatsJetStreamSourceConfig config) {
        this.config = config;
        this.deserializer = new AccessLogDeserializer();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        running = new AtomicBoolean(true);
        lastProcessedSequence = 0L;
        
        connectToNats();
    }

    private void connectToNats() throws IOException, InterruptedException, JetStreamApiException {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        
        LOG.info("Connecting to NATS JetStream: {} (subtask {}/{})", 
            config.getNatsUrl(), subtaskIndex + 1, numSubtasks);

        // Build connection options
        Options options = config.buildConnectionOptions()
            .connectionListener((conn, type) -> {
                LOG.info("NATS connection event: {}", type);
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

        // Connect to NATS
        natsConnection = Nats.connect(options);
        jetStream = natsConnection.jetStream();

        // Create or get consumer
        String consumerName = config.getDurable() + "-" + subtaskIndex;
        
        try {
            JetStreamManagement jsm = natsConnection.jetStreamManagement();
            
            // Ensure stream exists
            try {
                jsm.getStreamInfo(config.getStream());
            } catch (JetStreamApiException e) {
                if (e.getApiErrorCode() == 10059) {
                    // Stream not found, create it
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
            ConsumerConfiguration.Builder consumerConfigBuilder = ConsumerConfiguration.builder()
                .durable(consumerName)
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(config.getAckWait())
                .maxDeliver(5)
                .filterSubject(config.getSubject());

            // Set deliver policy
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
                case BY_START_TIME:
                    // BY_START_TIME requires additional configuration (startTime)
                    // Default to All if start time not configured
                    LOG.warn("BY_START_TIME deliver policy requires startTime configuration, defaulting to All");
                    consumerConfigBuilder.deliverPolicy(DeliverPolicy.All);
                    break;
            }

            ConsumerConfiguration consumerConfig = consumerConfigBuilder.build();

            // Create or update consumer
            try {
                jsm.addOrUpdateConsumer(config.getStream(), consumerConfig);
            } catch (JetStreamApiException e) {
                LOG.warn("Could not create consumer, it may already exist: {}", e.getMessage());
            }

            // Subscribe using pull-based consumer for better flow control
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .durable(consumerName)
                .stream(config.getStream())
                .build();

            subscription = jetStream.subscribe(config.getSubject(), pullOptions);
            
            LOG.info("Successfully subscribed to JetStream: stream={}, consumer={}", 
                config.getStream(), consumerName);

        } catch (Exception e) {
            LOG.error("Failed to setup JetStream consumer", e);
            throw e;
        }
    }

    @Override
    public void run(SourceContext<AccessLog> ctx) throws Exception {
        LOG.info("Starting NATS JetStream source");

        while (running.get()) {
            try {
                // Fetch batch of messages
                Iterator<Message> messages = subscription.iterate(
                    config.getBatchSize(), 
                    config.getMaxWait()
                );

                while (messages.hasNext() && running.get()) {
                    Message msg = messages.next();
                    
                    try {
                        // Deserialize the message
                        byte[] data = msg.getData();
                        AccessLog accessLog = deserializer.deserialize(data);
                        
                        if (accessLog != null) {
                            synchronized (ctx.getCheckpointLock()) {
                                ctx.collect(accessLog);
                                lastProcessedSequence = msg.metaData().streamSequence();
                            }
                        }
                        
                        // Acknowledge the message
                        msg.ack();
                        
                    } catch (Exception e) {
                        LOG.error("Error processing message: {}", 
                            new String(msg.getData(), StandardCharsets.UTF_8), e);
                        // NAK the message to retry
                        msg.nak();
                    }
                }
                
            } catch (IllegalStateException e) {
                if (running.get()) {
                    LOG.warn("Connection issue, attempting to reconnect...", e);
                    reconnect();
                }
            } catch (Exception e) {
                if (running.get()) {
                    LOG.error("Error fetching messages", e);
                    Thread.sleep(1000);
                }
            }
        }
    }

    private void reconnect() {
        try {
            close();
            Thread.sleep(2000);
            connectToNats();
        } catch (Exception e) {
            LOG.error("Failed to reconnect", e);
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling NATS JetStream source");
        running.set(false);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing NATS JetStream source");
        
        if (subscription != null) {
            try {
                subscription.drain(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOG.warn("Error draining subscription", e);
            }
        }
        
        if (natsConnection != null) {
            try {
                natsConnection.close();
            } catch (Exception e) {
                LOG.warn("Error closing NATS connection", e);
            }
        }
        
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedSequences.clear();
        checkpointedSequences.add(lastProcessedSequence);
        LOG.debug("Checkpointed sequence: {}", lastProcessedSequence);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedSequences = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>("nats-sequences", TypeInformation.of(Long.class))
        );

        if (context.isRestored()) {
            for (Long sequence : checkpointedSequences.get()) {
                lastProcessedSequence = sequence;
                LOG.info("Restored from checkpoint, last sequence: {}", lastProcessedSequence);
            }
        }
    }
}
