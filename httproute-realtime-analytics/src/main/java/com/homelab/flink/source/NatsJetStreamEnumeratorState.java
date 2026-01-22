package com.homelab.flink.source;

import java.io.Serializable;
import java.util.*;

/**
 * State of the NATS JetStream split enumerator.
 * Used for checkpointing and recovery of split assignment state.
 */
public class NatsJetStreamEnumeratorState implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final Set<String> assignedSplitIds;
    private final Map<Integer, Long> subtaskSequences;

    public NatsJetStreamEnumeratorState() {
        this.assignedSplitIds = new HashSet<>();
        this.subtaskSequences = new HashMap<>();
    }

    public NatsJetStreamEnumeratorState(Set<String> assignedSplitIds, Map<Integer, Long> subtaskSequences) {
        this.assignedSplitIds = new HashSet<>(assignedSplitIds);
        this.subtaskSequences = new HashMap<>(subtaskSequences);
    }

    public Set<String> getAssignedSplitIds() {
        return assignedSplitIds;
    }

    public Map<Integer, Long> getSubtaskSequences() {
        return subtaskSequences;
    }

    public void addAssignedSplit(String splitId) {
        assignedSplitIds.add(splitId);
    }

    public void updateSequence(int subtaskIndex, long sequence) {
        subtaskSequences.put(subtaskIndex, sequence);
    }

    @Override
    public String toString() {
        return "NatsJetStreamEnumeratorState{" +
                "assignedSplitIds=" + assignedSplitIds +
                ", subtaskSequences=" + subtaskSequences +
                '}';
    }
}
