package com.homelab.flink.source;

import com.homelab.flink.common.NatsJetStreamSourceConfig;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Split enumerator for NATS JetStream source.
 * 
 * Responsible for discovering and assigning splits to readers.
 * For NATS JetStream, each split corresponds to a durable consumer partition.
 */
public class NatsJetStreamSplitEnumerator 
        implements SplitEnumerator<NatsJetStreamSplit, NatsJetStreamEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(NatsJetStreamSplitEnumerator.class);

    private final SplitEnumeratorContext<NatsJetStreamSplit> context;
    private final NatsJetStreamSourceConfig config;
    private final NatsJetStreamEnumeratorState state;
    private final Map<Integer, List<NatsJetStreamSplit>> pendingSplits;

    public NatsJetStreamSplitEnumerator(
            SplitEnumeratorContext<NatsJetStreamSplit> context,
            NatsJetStreamSourceConfig config,
            @Nullable NatsJetStreamEnumeratorState restoredState) {
        this.context = context;
        this.config = config;
        this.state = restoredState != null ? restoredState : new NatsJetStreamEnumeratorState();
        this.pendingSplits = new HashMap<>();
    }

    @Override
    public void start() {
        LOG.info("Starting NATS JetStream split enumerator with parallelism {}", 
            context.currentParallelism());
        
        // Create splits for each parallel subtask
        int parallelism = context.currentParallelism();
        for (int i = 0; i < parallelism; i++) {
            String splitId = config.getDurable() + "-" + i;
            
            if (!state.getAssignedSplitIds().contains(splitId)) {
                long restoredSequence = state.getSubtaskSequences().getOrDefault(i, 0L);
                NatsJetStreamSplit split = new NatsJetStreamSplit(splitId, i, restoredSequence);
                
                LOG.info("Created split {} for subtask {} with restored sequence {}", 
                    splitId, i, restoredSequence);
                
                pendingSplits.computeIfAbsent(i, k -> new ArrayList<>()).add(split);
            }
        }
        
        // Assign pending splits to registered readers
        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Received split request from subtask {} at {}", subtaskId, requesterHostname);
        // Splits are assigned proactively, not on request
    }

    @Override
    public void addSplitsBack(List<NatsJetStreamSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        
        for (NatsJetStreamSplit split : splits) {
            state.getAssignedSplitIds().remove(split.splitId());
            pendingSplits.computeIfAbsent(split.getSubtaskIndex(), k -> new ArrayList<>()).add(split);
        }
        
        assignPendingSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader for subtask {}", subtaskId);
        assignPendingSplits();
    }

    private void assignPendingSplits() {
        Map<Integer, List<NatsJetStreamSplit>> toAssign = new HashMap<>();
        
        for (Map.Entry<Integer, List<NatsJetStreamSplit>> entry : pendingSplits.entrySet()) {
            int subtaskId = entry.getKey();
            List<NatsJetStreamSplit> splits = entry.getValue();
            
            if (!splits.isEmpty() && context.registeredReaders().containsKey(subtaskId)) {
                toAssign.put(subtaskId, new ArrayList<>(splits));
                splits.clear();
                
                for (NatsJetStreamSplit split : toAssign.get(subtaskId)) {
                    state.addAssignedSplit(split.splitId());
                }
            }
        }
        
        for (Map.Entry<Integer, List<NatsJetStreamSplit>> entry : toAssign.entrySet()) {
            LOG.info("Assigning {} splits to subtask {}", entry.getValue().size(), entry.getKey());
            context.assignSplits(new org.apache.flink.api.connector.source.SplitsAssignment<>(
                Collections.singletonMap(entry.getKey(), entry.getValue())
            ));
        }
    }

    @Override
    public NatsJetStreamEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting enumerator state at checkpoint {}", checkpointId);
        return state;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing NATS JetStream split enumerator");
    }
}
