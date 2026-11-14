package io.quorumdb.server.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Client request statistics measured on this node: per-second throughput for the last minute and
 * latency percentiles over a sliding window. Also published to Micrometer for Prometheus.
 * All numbers come from real requests; nothing is sampled or synthesised.
 */
public final class RequestMetrics {

    public enum Kind { WRITE, READ }

    private static final int WINDOW_SECONDS = 60;
    private static final int RESERVOIR = 8192;

    private final class Series {
        final long[] okPerSecond = new long[WINDOW_SECONDS];
        final long[] errPerSecond = new long[WINDOW_SECONDS];
        final long[] secondOf = new long[WINDOW_SECONDS];
        final long[] latencyMicros = new long[RESERVOIR];
        final long[] latencyAt = new long[RESERVOIR];
        int next;
        long totalOk;
        long totalErr;
        final Timer timer;

        Series(Timer timer) {
            this.timer = timer;
        }

        void record(long nowMs, long micros, boolean ok) {
            long second = nowMs / 1000;
            int slot = (int) (second % WINDOW_SECONDS);
            if (secondOf[slot] != second) {
                secondOf[slot] = second;
                okPerSecond[slot] = 0;
                errPerSecond[slot] = 0;
            }
            if (ok) {
                okPerSecond[slot]++;
                totalOk++;
                latencyMicros[next] = micros;
                latencyAt[next] = nowMs;
                next = (next + 1) % RESERVOIR;
                timer.record(micros, TimeUnit.MICROSECONDS);
            } else {
                errPerSecond[slot]++;
                totalErr++;
            }
        }
    }

    public record Window(long totalOk, long totalErrors, double opsPerSecond, double p50Ms, double p95Ms, double p99Ms,
                         double maxMs, int samples) {}

    public record Point(long second, long writes, long reads, long errors) {}

    private final Map<Kind, Series> series = new EnumMap<>(Kind.class);

    public RequestMetrics(MeterRegistry registry) {
        for (Kind k : Kind.values()) {
            series.put(k, new Series(Timer.builder("quorumdb.requests").tag("kind", k.name().toLowerCase())
                    .description("Successful client requests served by this node")
                    .publishPercentiles(0.5, 0.95, 0.99).register(registry)));
        }
    }

    public synchronized void record(Kind kind, long micros, boolean ok) {
        series.get(kind).record(System.currentTimeMillis(), micros, ok);
    }

    /** Stats over the last {@code seconds} complete seconds (and the current partial one for latency). */
    public synchronized Window window(Kind kind, int seconds) {
        Series s = series.get(kind);
        long now = System.currentTimeMillis();
        long nowSecond = now / 1000;
        long ok = 0;
        for (int i = 1; i <= seconds; i++) {
            long sec = nowSecond - i;
            int slot = (int) (sec % WINDOW_SECONDS);
            if (s.secondOf[slot] == sec) ok += s.okPerSecond[slot];
        }
        long[] recent = new long[RESERVOIR];
        int n = 0;
        for (int i = 0; i < RESERVOIR; i++) {
            if (s.latencyAt[i] > 0 && now - s.latencyAt[i] <= seconds * 1000L) recent[n++] = s.latencyMicros[i];
        }
        long[] sorted = Arrays.copyOf(recent, n);
        Arrays.sort(sorted);
        return new Window(s.totalOk, s.totalErr, (double) ok / seconds, pct(sorted, 0.50), pct(sorted, 0.95),
                pct(sorted, 0.99), n == 0 ? 0 : sorted[n - 1] / 1000.0, n);
    }

    /** One point per second for the last minute, oldest first. */
    public synchronized List<Point> series() {
        long nowSecond = System.currentTimeMillis() / 1000;
        Series w = series.get(Kind.WRITE);
        Series r = series.get(Kind.READ);
        Point[] points = new Point[WINDOW_SECONDS - 1];
        for (int i = WINDOW_SECONDS - 1; i >= 1; i--) {
            long sec = nowSecond - i;
            int slot = (int) (sec % WINDOW_SECONDS);
            long writes = w.secondOf[slot] == sec ? w.okPerSecond[slot] : 0;
            long reads = r.secondOf[slot] == sec ? r.okPerSecond[slot] : 0;
            long errors = (w.secondOf[slot] == sec ? w.errPerSecond[slot] : 0) + (r.secondOf[slot] == sec ? r.errPerSecond[slot] : 0);
            points[WINDOW_SECONDS - 1 - i] = new Point(sec * 1000, writes, reads, errors);
        }
        return List.of(points);
    }

    public synchronized Map<String, Window> summary(int seconds) {
        Map<String, Window> out = new LinkedHashMap<>();
        for (Kind k : Kind.values()) out.put(k.name().toLowerCase(), window(k, seconds));
        return out;
    }

    private static double pct(long[] sorted, double q) {
        if (sorted.length == 0) return 0;
        int rank = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank))] / 1000.0;
    }
}
