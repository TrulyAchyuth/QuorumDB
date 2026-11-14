package io.quorumdb.server.node;

import io.quorumdb.raft.RaftEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded in-memory ring of this node's events. Sequence numbers are assigned here, increase by one
 * per event for the lifetime of the process (also across in-process Raft restarts), so a reader can
 * tell exactly which events it missed.
 */
public final class EventLog {
    /** High-volume event types; everything else (elections, faults, lifecycle, snapshots) is "rare". */
    private static final java.util.Set<RaftEvent.Type> FREQUENT = java.util.EnumSet.of(RaftEvent.Type.ENTRY_APPENDED,
            RaftEvent.Type.ENTRY_COMMITTED, RaftEvent.Type.ENTRY_APPLIED, RaftEvent.Type.HEARTBEAT_SENT,
            RaftEvent.Type.VOTE_REQUESTED, RaftEvent.Type.MESSAGE_SENT, RaftEvent.Type.MESSAGE_DELIVERED,
            RaftEvent.Type.MESSAGE_DROPPED);

    private final RaftEvent[] ring;
    /**
     * Rare events are also kept in their own ring: under write load the main ring holds only seconds of
     * history, which would otherwise push elections and faults out almost immediately.
     */
    private final RaftEvent[] rare;
    private int rareNext;
    private int rareCount;
    private long nextSeq = 1;

    public record Page(List<RaftEvent> events, long oldestSeq, long latestSeq, boolean truncated) {}

    public EventLog(int capacity) {
        this.ring = new RaftEvent[capacity];
        this.rare = new RaftEvent[Math.max(16, capacity / 10)];
    }

    public synchronized RaftEvent append(RaftEvent e) {
        RaftEvent stamped = new RaftEvent(nextSeq, e.timeMs(), e.node(), e.type(), e.term(), e.index(), e.entryTerm(),
                e.checksum(), e.peer(), e.detail());
        ring[(int) (nextSeq % ring.length)] = stamped;
        nextSeq++;
        if (!FREQUENT.contains(stamped.type())) {
            rare[rareNext] = stamped;
            rareNext = (rareNext + 1) % rare.length;
            rareCount = Math.min(rareCount + 1, rare.length);
        }
        return stamped;
    }

    /**
     * @param afterSeq return events with seq &gt; afterSeq
     * @return {@code truncated} is true if events after {@code afterSeq} were already overwritten
     */
    public synchronized Page after(long afterSeq, int limit) {
        long latest = nextSeq - 1;
        long oldest = Math.max(1, nextSeq - ring.length);
        long from = Math.max(afterSeq + 1, oldest);
        List<RaftEvent> out = new ArrayList<>();
        for (long s = from; s <= latest && out.size() < limit; s++) out.add(ring[(int) (s % ring.length)]);
        return new Page(out, oldest, latest, afterSeq + 1 < oldest && latest > 0);
    }

    /** The most recent {@code limit} events, oldest first. */
    public synchronized List<RaftEvent> latest(int limit) {
        long latest = nextSeq - 1;
        return after(Math.max(0, latest - limit), limit).events();
    }

    /** The most recent {@code limit} events whose type is in {@code types}, oldest first. */
    public synchronized List<RaftEvent> latest(int limit, java.util.Set<RaftEvent.Type> types) {
        java.util.ArrayDeque<RaftEvent> out = new java.util.ArrayDeque<>();
        if (java.util.Collections.disjoint(types, FREQUENT)) {
            // Only rare types requested: answer from the long-lived rare ring.
            for (int i = 1; i <= rareCount && out.size() < limit; i++) {
                RaftEvent e = rare[Math.floorMod(rareNext - i, rare.length)];
                if (types.contains(e.type())) out.addFirst(e);
            }
            return new ArrayList<>(out);
        }
        long oldest = Math.max(1, nextSeq - ring.length);
        for (long s = nextSeq - 1; s >= oldest && out.size() < limit; s--) {
            RaftEvent e = ring[(int) (s % ring.length)];
            if (types.contains(e.type())) out.addFirst(e);
        }
        return new ArrayList<>(out);
    }

    public synchronized long latestSeq() {
        return nextSeq - 1;
    }
}
