package com.homelab.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a split for the NATS JetStream source.
 * Each split represents a partition of work for the source reader.
 * 
 * For NATS JetStream, each split corresponds to a durable consumer partition.
 */
public class NatsJetStreamSplit implements SourceSplit, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String splitId;
    private final int subtaskIndex;
    private long lastProcessedSequence;

    public NatsJetStreamSplit(String splitId, int subtaskIndex) {
        this.splitId = splitId;
        this.subtaskIndex = subtaskIndex;
        this.lastProcessedSequence = 0L;
    }

    public NatsJetStreamSplit(String splitId, int subtaskIndex, long lastProcessedSequence) {
        this.splitId = splitId;
        this.subtaskIndex = subtaskIndex;
        this.lastProcessedSequence = lastProcessedSequence;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public long getLastProcessedSequence() {
        return lastProcessedSequence;
    }

    public void setLastProcessedSequence(long sequence) {
        this.lastProcessedSequence = sequence;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NatsJetStreamSplit that = (NatsJetStreamSplit) o;
        return subtaskIndex == that.subtaskIndex && 
               lastProcessedSequence == that.lastProcessedSequence && 
               Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, subtaskIndex, lastProcessedSequence);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSplit{" +
                "splitId='" + splitId + '\'' +
                ", subtaskIndex=" + subtaskIndex +
                ", lastProcessedSequence=" + lastProcessedSequence +
                '}';
    }
}
