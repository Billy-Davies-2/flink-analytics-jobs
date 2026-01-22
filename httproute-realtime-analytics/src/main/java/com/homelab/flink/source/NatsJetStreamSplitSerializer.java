package com.homelab.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for NATS JetStream splits.
 * Used by Flink for checkpointing and recovery of split state.
 */
public class NatsJetStreamSplitSerializer implements SimpleVersionedSerializer<NatsJetStreamSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsJetStreamSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            
            byte[] splitIdBytes = split.splitId().getBytes(StandardCharsets.UTF_8);
            out.writeInt(splitIdBytes.length);
            out.write(splitIdBytes);
            out.writeInt(split.getSubtaskIndex());
            out.writeLong(split.getLastProcessedSequence());
            
            return baos.toByteArray();
        }
    }

    @Override
    public NatsJetStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            
            int splitIdLength = in.readInt();
            byte[] splitIdBytes = new byte[splitIdLength];
            in.readFully(splitIdBytes);
            String splitId = new String(splitIdBytes, StandardCharsets.UTF_8);
            
            int subtaskIndex = in.readInt();
            long lastProcessedSequence = in.readLong();
            
            return new NatsJetStreamSplit(splitId, subtaskIndex, lastProcessedSequence);
        }
    }
}
