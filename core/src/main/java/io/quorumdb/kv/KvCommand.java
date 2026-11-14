package io.quorumdb.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A write command for the key-value state machine: one or more operations applied atomically.
 *
 * <p>{@code clientId}/{@code sequence} make retries idempotent: the state machine remembers the
 * last sequence it executed per client and answers a repeated command from its cache instead of
 * executing it twice (Raft §8). An empty client id disables de-duplication.
 */
public record KvCommand(String clientId, long sequence, List<Op> ops) {

    public enum OpType { PUT, DELETE }

    /** Sentinel: no precondition on the key's version. */
    public static final long ANY_VERSION = -1;

    /**
     * @param expectedVersion {@link #ANY_VERSION} for unconditional; 0 = key must not exist;
     *                        otherwise the key's current version must equal it (compare-and-set)
     */
    public record Op(OpType type, String key, String value, long expectedVersion) {
        public static Op put(String key, String value) {
            return new Op(OpType.PUT, key, value, ANY_VERSION);
        }

        public static Op delete(String key) {
            return new Op(OpType.DELETE, key, null, ANY_VERSION);
        }
    }

    public KvCommand {
        clientId = clientId == null ? "" : clientId;
        ops = List.copyOf(ops);
        if (ops.isEmpty()) throw new IllegalArgumentException("a command needs at least one operation");
    }

    public static KvCommand single(String clientId, long sequence, Op op) {
        return new KvCommand(clientId, sequence, List.of(op));
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(1); // format version
            out.writeUTF(clientId);
            out.writeLong(sequence);
            out.writeInt(ops.size());
            for (Op op : ops) {
                out.writeByte(op.type().ordinal());
                writeString(out, op.key());
                out.writeBoolean(op.value() != null);
                if (op.value() != null) writeString(out, op.value());
                out.writeLong(op.expectedVersion());
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static KvCommand decode(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int version = in.readByte();
            if (version != 1) throw new IllegalArgumentException("unsupported command format " + version);
            String clientId = in.readUTF();
            long sequence = in.readLong();
            int count = in.readInt();
            if (count < 1 || count > data.length) throw new IllegalArgumentException("bad op count " + count);
            List<Op> ops = new ArrayList<>(count);
            OpType[] types = OpType.values();
            for (int i = 0; i < count; i++) {
                int type = in.readByte();
                if (type < 0 || type >= types.length) throw new IllegalArgumentException("bad op type " + type);
                String key = readString(in);
                String value = in.readBoolean() ? readString(in) : null;
                ops.add(new Op(types[type], key, value, in.readLong()));
            }
            return new KvCommand(clientId, sequence, ops);
        } catch (IOException e) {
            throw new IllegalArgumentException("truncated command", e);
        }
    }

    /** Length-prefixed UTF-8 (DataOutput.writeUTF is limited to 64 KiB). */
    static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) throw new IOException("bad string length " + length);
        byte[] b = new byte[length];
        in.readFully(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }
}
