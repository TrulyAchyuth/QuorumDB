package io.quorumdb.kv;

import io.quorumdb.raft.Collaborators.StateMachine;
import io.quorumdb.raft.LogEntry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * The replicated key-value store. Fully deterministic: its state is a pure function of the
 * sequence of applied log entries, which is what makes replicas converge.
 *
 * <p>Each key carries a <em>version</em>: the log index of the command that last wrote it.
 * Versions give clients compare-and-set and give the linearizability checker something exact
 * to verify.
 *
 * <p>Not thread-safe: mutated and read on the owning Raft node's thread only.
 */
public final class KvStateMachine implements StateMachine {

    /** A stored value and the log index that wrote it. */
    public record Versioned(String value, long version) {}

    private record Session(long sequence, KvResult result) {}

    private static final int SNAPSHOT_MAGIC = 0x51444253; // "QDBS"
    private static final byte[] REJECTED = new KvResult(false, false, java.util.List.of()).encode();

    private final TreeMap<String, Versioned> data = new TreeMap<>();
    /** Insertion-ordered so eviction is deterministic across replicas. */
    private final LinkedHashMap<String, Session> sessions = new LinkedHashMap<>();
    private final int maxSessions;
    private long lastAppliedIndex;

    public KvStateMachine() {
        this(10_000);
    }

    public KvStateMachine(int maxSessions) {
        this.maxSessions = maxSessions;
    }

    @Override
    public byte[] apply(LogEntry entry) {
        lastAppliedIndex = entry.index();
        KvCommand command;
        try {
            command = KvCommand.decode(entry.data());
        } catch (IllegalArgumentException malformed) {
            // A committed entry that is not a valid command is rejected the same way on every replica
            // (decoding is deterministic), so state stays identical. Halting instead would let a single
            // malformed entry take down the whole cluster, forever, since it is replayed on restart.
            return REJECTED;
        }
        return execute(command, entry.index()).encode();
    }

    private KvResult execute(KvCommand command, long index) {
        boolean tracked = !command.clientId().isEmpty();
        if (tracked) {
            Session session = sessions.get(command.clientId());
            if (session != null && command.sequence() <= session.sequence()) {
                // A retry of something already executed: answer from the cache, do not re-apply.
                // (For an older sequence the cached result is the newest one we have; clients
                // only retry their latest request, so this only matters for misbehaving callers.)
                return session.result().asDuplicate();
            }
        }
        KvResult result = applyOps(command.ops(), index);
        if (tracked) {
            sessions.remove(command.clientId());
            sessions.put(command.clientId(), new Session(command.sequence(), result));
            if (sessions.size() > maxSessions) {
                Iterator<String> oldest = sessions.keySet().iterator();
                oldest.next();
                oldest.remove();
            }
        }
        return result;
    }

    private KvResult applyOps(List<KvCommand.Op> ops, long index) {
        // All-or-nothing: check every precondition before touching anything.
        for (KvCommand.Op op : ops) {
            if (op.expectedVersion() == KvCommand.ANY_VERSION) continue;
            Versioned current = data.get(op.key());
            long currentVersion = current == null ? 0 : current.version();
            if (currentVersion != op.expectedVersion()) {
                List<KvResult.OpResult> results = new ArrayList<>(ops.size());
                for (KvCommand.Op each : ops) {
                    Versioned v = data.get(each.key());
                    results.add(new KvResult.OpResult(v != null, v == null ? 0 : v.version()));
                }
                return new KvResult(false, false, results);
            }
        }
        List<KvResult.OpResult> results = new ArrayList<>(ops.size());
        for (KvCommand.Op op : ops) {
            Versioned previous = switch (op.type()) {
                case PUT -> data.put(op.key(), new Versioned(op.value(), index));
                case DELETE -> data.remove(op.key());
            };
            results.add(new KvResult.OpResult(previous != null, op.type() == KvCommand.OpType.PUT ? index : 0));
        }
        return new KvResult(true, false, results);
    }

    // ---- queries (never go through the log; consistency is decided by the caller) ----

    public Versioned get(String key) {
        return data.get(key);
    }

    public boolean contains(String key) {
        return data.containsKey(key);
    }

    public int size() {
        return data.size();
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** Keys starting with {@code prefix}, in order, at most {@code limit}. */
    public List<Map.Entry<String, Versioned>> scan(String prefix, int limit) {
        NavigableMap<String, Versioned> tail = prefix == null || prefix.isEmpty() ? data : data.tailMap(prefix, true);
        List<Map.Entry<String, Versioned>> out = new ArrayList<>();
        for (Map.Entry<String, Versioned> e : tail.entrySet()) {
            if (prefix != null && !e.getKey().startsWith(prefix)) break;
            if (out.size() >= limit) break;
            out.add(Map.entry(e.getKey(), e.getValue()));
        }
        return out;
    }

    /** Fingerprint of the full key/value contents; equal on replicas that applied the same prefix. */
    public long digest() {
        CRC32C crc = new CRC32C();
        for (Map.Entry<String, Versioned> e : data.entrySet()) {
            crc.update(e.getKey().getBytes(StandardCharsets.UTF_8));
            crc.update(0);
            crc.update(e.getValue().value().getBytes(StandardCharsets.UTF_8));
            crc.update(0);
            long v = e.getValue().version();
            for (int shift = 56; shift >= 0; shift -= 8) crc.update((int) (v >>> shift) & 0xff);
        }
        return crc.getValue();
    }

    // ---- snapshots ----

    @Override
    public byte[] snapshot() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(SNAPSHOT_MAGIC);
            out.writeInt(1);
            out.writeLong(lastAppliedIndex);
            out.writeInt(data.size());
            for (Map.Entry<String, Versioned> e : data.entrySet()) {
                KvCommand.writeString(out, e.getKey());
                KvCommand.writeString(out, e.getValue().value());
                out.writeLong(e.getValue().version());
            }
            out.writeInt(sessions.size());
            for (Map.Entry<String, Session> e : sessions.entrySet()) {
                KvCommand.writeString(out, e.getKey());
                out.writeLong(e.getValue().sequence());
                byte[] result = e.getValue().result().encode();
                out.writeInt(result.length);
                out.write(result);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void restore(byte[] snapshot) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot));
            if (in.readInt() != SNAPSHOT_MAGIC) throw new IllegalArgumentException("not a QuorumDB KV snapshot");
            int version = in.readInt();
            if (version != 1) throw new IllegalArgumentException("unsupported snapshot version " + version);
            long applied = in.readLong();
            TreeMap<String, Versioned> newData = new TreeMap<>();
            int entries = in.readInt();
            for (int i = 0; i < entries; i++) {
                String key = KvCommand.readString(in);
                String value = KvCommand.readString(in);
                newData.put(key, new Versioned(value, in.readLong()));
            }
            LinkedHashMap<String, Session> newSessions = new LinkedHashMap<>();
            int sessionCount = in.readInt();
            for (int i = 0; i < sessionCount; i++) {
                String client = KvCommand.readString(in);
                long sequence = in.readLong();
                byte[] result = new byte[in.readInt()];
                in.readFully(result);
                newSessions.put(client, new Session(sequence, KvResult.decode(result)));
            }
            data.clear();
            data.putAll(newData);
            sessions.clear();
            sessions.putAll(newSessions);
            lastAppliedIndex = applied;
        } catch (IOException e) {
            throw new IllegalArgumentException("corrupt KV snapshot", e);
        }
    }
}
