package io.quorumdb.raft;

import java.util.zip.CRC32C;

/**
 * One entry of the replicated log. Entries are immutable once created; the pair
 * (index, term) uniquely identifies an entry cluster-wide (Raft Log Matching property).
 */
public record LogEntry(long index, long term, Type type, byte[] data) {

    public enum Type {
        /** Appended by every new leader so it can commit an entry from its own term (Raft §5.4.2, §8). */
        NOOP,
        /** An opaque state machine command. */
        COMMAND,
        /**
         * The complete new set of voting members, one per line (single-server membership change, Raft dissertation
         * §4.1). Each line is a member id, optionally followed by {@code =} and how to reach it. Takes effect when
         * appended. Appended last so existing ordinals (stored in the WAL and on the wire) do not change.
         */
        CONFIG;

        public static Type fromOrdinal(int ordinal) {
            Type[] values = values();
            if (ordinal < 0 || ordinal >= values.length) {
                throw new IllegalArgumentException("unknown entry type " + ordinal);
            }
            return values[ordinal];
        }
    }

    public LogEntry {
        if (index < 1) throw new IllegalArgumentException("index must be >= 1: " + index);
        if (term < 1) throw new IllegalArgumentException("term must be >= 1: " + term);
        if (type == null || data == null) throw new IllegalArgumentException("type and data are required");
    }

    /**
     * Content fingerprint over (term, type, data). Used by the safety checker to compare
     * what different nodes committed/applied at an index without shipping the payload.
     */
    public long checksum() {
        CRC32C crc = new CRC32C();
        for (int shift = 56; shift >= 0; shift -= 8) crc.update((int) (term >>> shift) & 0xff);
        crc.update(type.ordinal());
        crc.update(data, 0, data.length);
        return crc.getValue();
    }

    /** Approximate encoded size, used for batching limits. */
    public int sizeBytes() {
        return 24 + data.length;
    }

    @Override
    public String toString() {
        return "LogEntry[" + index + "@" + term + " " + type + " " + data.length + "B]";
    }
}
