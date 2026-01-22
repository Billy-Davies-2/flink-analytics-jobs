package com.homelab.flink.source;

import com.homelab.flink.common.NatsJetStreamSourceConfig;
import com.homelab.flink.model.AccessLog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NATS JetStream source for Flink (Source V2 API).
 * 
 * This source consumes messages from a NATS JetStream stream and emits them
 * as AccessLog objects for processing in the Flink pipeline.
 * 
 * Features:
 * - Durable consumer support for exactly-once semantics
 * - Automatic reconnection handling
 * - Checkpoint-based state recovery
 * - Parallel consumption support via split-based architecture
 * 
 * This implements the FLIP-27 Source API introduced in Flink 1.11 and
 * required in Flink 2.0+ (after RichParallelSourceFunction was removed).
 */
public class NatsJetStreamSource implements Source<AccessLog, NatsJetStreamSplit, NatsJetStreamEnumeratorState> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSource.class);

    private final NatsJetStreamSourceConfig config;

    public NatsJetStreamSource(NatsJetStreamSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        // NATS JetStream is an unbounded streaming source
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<NatsJetStreamSplit, NatsJetStreamEnumeratorState> createEnumerator(
            SplitEnumeratorContext<NatsJetStreamSplit> enumContext) throws Exception {
        LOG.info("Creating new NATS JetStream split enumerator");
        return new NatsJetStreamSplitEnumerator(enumContext, config, null);
    }

    @Override
    public SplitEnumerator<NatsJetStreamSplit, NatsJetStreamEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<NatsJetStreamSplit> enumContext,
            NatsJetStreamEnumeratorState checkpoint) throws Exception {
        LOG.info("Restoring NATS JetStream split enumerator from checkpoint");
        return new NatsJetStreamSplitEnumerator(enumContext, config, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NatsJetStreamSplit> getSplitSerializer() {
        return new NatsJetStreamSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NatsJetStreamEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new NatsJetStreamEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<AccessLog, NatsJetStreamSplit> createReader(SourceReaderContext readerContext) 
            throws Exception {
        LOG.info("Creating NATS JetStream source reader for subtask {}", 
            readerContext.getIndexOfSubtask());
        return new NatsJetStreamSourceReader(readerContext, config);
    }

    /**
     * Returns the TypeInformation for the AccessLog output type.
     * This is used by Flink for serialization.
     */
    public TypeInformation<AccessLog> getProducedType() {
        return TypeInformation.of(AccessLog.class);
    }
}
