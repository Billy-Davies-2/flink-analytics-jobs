package com.homelab.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Serializer for NATS JetStream enumerator state.
 * Used by Flink for checkpointing and recovery of enumerator state.
 */
public class NatsJetStreamEnumeratorStateSerializer 
        implements SimpleVersionedSerializer<NatsJetStreamEnumeratorState> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NatsJetStreamEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            
            // Serialize assigned split IDs
            Set<String> assignedSplitIds = state.getAssignedSplitIds();
            out.writeInt(assignedSplitIds.size());
            for (String splitId : assignedSplitIds) {
                byte[] splitIdBytes = splitId.getBytes(StandardCharsets.UTF_8);
                out.writeInt(splitIdBytes.length);
                out.write(splitIdBytes);
            }
            
            // Serialize subtask sequences
            Map<Integer, Long> subtaskSequences = state.getSubtaskSequences();
            out.writeInt(subtaskSequences.size());
            for (Map.Entry<Integer, Long> entry : subtaskSequences.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
            }
            
            return baos.toByteArray();
        }
    }

    @Override
    public NatsJetStreamEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            
            // Deserialize assigned split IDs
            int numSplitIds = in.readInt();
            Set<String> assignedSplitIds = new HashSet<>();
            for (int i = 0; i < numSplitIds; i++) {
                int splitIdLength = in.readInt();
                byte[] splitIdBytes = new byte[splitIdLength];
                in.readFully(splitIdBytes);
                assignedSplitIds.add(new String(splitIdBytes, StandardCharsets.UTF_8));
            }
            
            // Deserialize subtask sequences
            int numSequences = in.readInt();
            Map<Integer, Long> subtaskSequences = new HashMap<>();
            for (int i = 0; i < numSequences; i++) {
                int subtaskIndex = in.readInt();
                long sequence = in.readLong();
                subtaskSequences.put(subtaskIndex, sequence);
            }
            
            return new NatsJetStreamEnumeratorState(assignedSplitIds, subtaskSequences);
        }
    }
}
