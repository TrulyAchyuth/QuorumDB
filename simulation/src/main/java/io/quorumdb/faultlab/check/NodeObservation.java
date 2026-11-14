package io.quorumdb.faultlab.check;

import java.util.List;

/**
 * What the safety checker can see of one node at one instant: its durable log (as index/term/
 * checksum triples) and a few volatile fields. Produced by the simulator directly and, for live
 * clusters, from each node's diagnostic API.
 *
 * @param reachable false if the node was down/unreachable; then only durable fields are meaningful
 */
public record NodeObservation(String node, long timeMs, long term, String role, long commitIndex,
                              long lastApplied, long snapshotIndex, long snapshotTerm, List<LogRef> log,
                              long stateDigest, boolean reachable) {

    public record LogRef(long index, long term, long checksum) {}

    public NodeObservation {
        log = List.copyOf(log);
    }

    /** @return the entry at {@code index}, or null if it is compacted or beyond the log */
    public LogRef at(long index) {
        long offset = index - snapshotIndex - 1;
        if (offset < 0 || offset >= log.size()) return null;
        return log.get((int) offset);
    }

    public long lastIndex() {
        return snapshotIndex + log.size();
    }
}
