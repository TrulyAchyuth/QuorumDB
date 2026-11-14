package io.quorumdb.server.node;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** The most recent client operations handled by this node, for the Operations Explorer. */
public final class OperationLog {

    /**
     * @param status      OK, NOT_FOUND, CONFLICT, FORWARDED, NOT_LEADER, BUSY, UNKNOWN, TIMEOUT, ERROR, REJECTED
     * @param logIndex    log index of a write (0 for reads and failed writes)
     * @param readIndex   index the read was served at (applied index for stale reads)
     * @param committed   whether a write is known committed (true only when acknowledged)
     */
    public record Operation(long id, long timeMs, String type, String key, String consistency, String status,
                            long latencyMicros, long logIndex, long term, long readIndex, boolean committed,
                            String servedBy, String clientId) {}

    private final int capacity;
    private final ArrayDeque<Operation> ops = new ArrayDeque<>();
    private long nextId = 1;

    public OperationLog(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void add(long timeMs, String type, String key, String consistency, String status,
                                 long latencyMicros, long logIndex, long term, long readIndex, boolean committed,
                                 String servedBy, String clientId) {
        ops.addLast(new Operation(nextId++, timeMs, type, key, consistency, status, latencyMicros, logIndex, term,
                readIndex, committed, servedBy, clientId));
        while (ops.size() > capacity) ops.removeFirst();
    }

    /** Newest first. */
    public synchronized List<Operation> recent(int limit, String type, String status) {
        List<Operation> out = new ArrayList<>();
        var it = ops.descendingIterator();
        while (it.hasNext() && out.size() < limit) {
            Operation o = it.next();
            if (type != null && !type.equalsIgnoreCase(o.type())) continue;
            if (status != null && !status.equalsIgnoreCase(o.status())) continue;
            out.add(o);
        }
        return out;
    }
}
