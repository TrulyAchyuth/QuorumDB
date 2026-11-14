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
 * Outcome of applying a {@link KvCommand}.
 *
 * @param applied   false if a version precondition failed (then nothing in the command was applied)
 * @param duplicate true if this command had already been executed and the cached result is returned
 * @param ops       per-operation details, in command order
 */
public record KvResult(boolean applied, boolean duplicate, List<OpResult> ops) {

    /**
     * @param existed whether the key existed before the operation
     * @param version the key's version after the operation (0 if it does not exist afterwards)
     */
    public record OpResult(boolean existed, long version) {}

    public KvResult {
        ops = List.copyOf(ops);
    }

    public KvResult asDuplicate() {
        return new KvResult(applied, true, ops);
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeBoolean(applied);
            out.writeBoolean(duplicate);
            out.writeInt(ops.size());
            for (OpResult r : ops) {
                out.writeBoolean(r.existed());
                out.writeLong(r.version());
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static KvResult decode(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            boolean applied = in.readBoolean();
            boolean duplicate = in.readBoolean();
            int count = in.readInt();
            List<OpResult> ops = new ArrayList<>(Math.max(0, Math.min(count, 1024)));
            for (int i = 0; i < count; i++) ops.add(new OpResult(in.readBoolean(), in.readLong()));
            return new KvResult(applied, duplicate, ops);
        } catch (IOException e) {
            throw new IllegalArgumentException("truncated result", e);
        }
    }
}
