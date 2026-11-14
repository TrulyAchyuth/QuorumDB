package io.quorumdb.raft;

/**
 * The small interfaces through which {@link RaftNode} touches the outside world. Swapping these
 * is what lets one consensus implementation run both on real threads/gRPC/disk and inside the
 * deterministic FaultLab simulator.
 */
public final class Collaborators {
    private Collaborators() {}

    /** Time source and timer facility. Callbacks must run on the node's single event thread. */
    public interface Scheduler {
        long nowMs();

        Cancellable schedule(long delayMs, Runnable task);
    }

    @FunctionalInterface
    public interface Cancellable {
        void cancel();
    }

    /** Best-effort, unordered, at-most-or-more-than-once message delivery. Must never block. */
    @FunctionalInterface
    public interface Transport {
        void send(String to, RaftMessage message);
    }

    /** Source of election timeout jitter; seeded in simulation for reproducibility. */
    @FunctionalInterface
    public interface RandomSource {
        /** @return uniformly distributed value in [0, bound) */
        long nextLong(long bound);
    }

    /**
     * Deterministic state machine. {@code apply} is invoked exactly once per committed COMMAND entry,
     * in log order, on the node thread.
     */
    public interface StateMachine {
        byte[] apply(LogEntry entry);

        byte[] snapshot();

        void restore(byte[] snapshot);
    }

    @FunctionalInterface
    public interface EventListener {
        void onEvent(RaftEvent event);

        EventListener NONE = e -> {};
    }
}
