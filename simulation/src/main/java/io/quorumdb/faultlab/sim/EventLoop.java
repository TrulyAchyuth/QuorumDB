package io.quorumdb.faultlab.sim;

import java.util.PriorityQueue;

/**
 * The simulator's notion of time: a priority queue of tasks ordered by (virtual time, insertion
 * order). Nothing in a simulation reads the wall clock or runs on another thread, so a run is a
 * pure function of its inputs and seed.
 */
public final class EventLoop {
    private boolean halted;

    public static final class Task implements Comparable<Task> {
        final long timeMs;
        final long order;
        final Runnable body;
        boolean cancelled;

        Task(long timeMs, long order, Runnable body) {
            this.timeMs = timeMs;
            this.order = order;
            this.body = body;
        }

        public void cancel() {
            cancelled = true;
        }

        @Override
        public int compareTo(Task other) {
            int byTime = Long.compare(timeMs, other.timeMs);
            return byTime != 0 ? byTime : Long.compare(order, other.order);
        }
    }

    private final PriorityQueue<Task> queue = new PriorityQueue<>();
    private long nowMs;
    private long nextOrder;
    private long executed;

    public long nowMs() {
        return nowMs;
    }

    public long executedTasks() {
        return executed;
    }

    public Task schedule(long delayMs, Runnable body) {
        Task task = new Task(nowMs + Math.max(0, delayMs), nextOrder++, body);
        queue.add(task);
        return task;
    }

    /** Runs every task scheduled at or before {@code untilMs}, then sets the clock to {@code untilMs}. */
    /** Makes the current and every later {@link #runUntil} return after the task that is running now. */
    public void halt() {
        halted = true;
    }

    public boolean halted() {
        return halted;
    }

    public void runUntil(long untilMs) {
        while (!halted && !queue.isEmpty() && queue.peek().timeMs <= untilMs) {
            Task task = queue.poll();
            if (task.cancelled) continue;
            nowMs = task.timeMs;
            executed++;
            task.body.run();
        }
        if (!halted) nowMs = Math.max(nowMs, untilMs);
    }
}
