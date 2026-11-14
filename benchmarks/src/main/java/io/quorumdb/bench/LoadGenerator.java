package io.quorumdb.bench;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Closed-loop load generator for QuorumDB.
 *
 * <p>{@code concurrency} workers each send one request, wait for the answer, and immediately send the
 * next (no think time), for {@code warmup + duration} seconds. Only requests that <em>start</em> after
 * warm-up are measured. Latency is the client-side round trip (send to last byte of the response).
 * Writes go to the leader (discovered via {@code /api/v1/leader}, rediscovered after an error), like a
 * leader-aware client; with {@code --spread} they go round-robin to all nodes and followers forward them.
 *
 * <pre>
 * java -jar benchmarks.jar --targets http://localhost:8081,http://localhost:8082 --concurrency 32 \
 *      --duration 20 --warmup 5 --read-ratio 0.0 --value-bytes 128 --keys 10000 --out result.json
 * </pre>
 *
 * Optional: {@code --label}, {@code --consistency linearizable|leader|stale}, {@code --spread},
 * {@code --timeline} (per-100ms success/error counts, used to measure unavailability during faults).
 */
public final class LoadGenerator {

    record Options(List<String> targets, int concurrency, int durationS, int warmupS, double readRatio, int valueBytes,
                   int keys, String consistency, boolean spread, boolean timeline, String label, Path out) {}

    /** Per-worker recording; merged at the end so workers never contend. */
    static final class Recorder {
        long[] writeLatencyMicros = new long[1 << 16];
        int writes;
        long[] readLatencyMicros = new long[1 << 16];
        int reads;
        long writeErrors;
        long readErrors;
        final Map<String, Long> errorCodes = new LinkedHashMap<>();
        long[] okPerBucket;
        long[] errPerBucket;

        Recorder(int buckets) {
            okPerBucket = new long[buckets];
            errPerBucket = new long[buckets];
        }

        void ok(boolean write, long micros, int bucket) {
            if (write) {
                if (writes == writeLatencyMicros.length) writeLatencyMicros = Arrays.copyOf(writeLatencyMicros, writes * 2);
                writeLatencyMicros[writes++] = micros;
            } else {
                if (reads == readLatencyMicros.length) readLatencyMicros = Arrays.copyOf(readLatencyMicros, reads * 2);
                readLatencyMicros[reads++] = micros;
            }
            if (bucket >= 0 && bucket < okPerBucket.length) okPerBucket[bucket]++;
        }

        void error(boolean write, String code, int bucket) {
            if (write) writeErrors++; else readErrors++;
            errorCodes.merge(code, 1L, Long::sum);
            if (bucket >= 0 && bucket < errPerBucket.length) errPerBucket[bucket]++;
        }
    }

    public static void main(String[] args) throws Exception {
        Options o = parse(args);
        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(1)).executor(Executors.newVirtualThreadPerTaskExecutor()).build();
        AtomicReference<String> leader = new AtomicReference<>(discoverLeader(http, o.targets()));
        String value = "v".repeat(o.valueBytes());
        String body = "{\"value\":\"" + value + "\"}";

        // Pre-populate keys for reads so GETs hit existing data.
        if (o.readRatio() > 0) {
            int preload = Math.min(o.keys(), 2_000);
            for (int i = 0; i < preload; i++) {
                send(http, HttpRequest.newBuilder(URI.create(leader.get() + "/api/v1/kv/bench-" + i))
                        .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(body)));
            }
        }

        long startNanos = System.nanoTime();
        long measureFrom = startNanos + TimeUnit.SECONDS.toNanos(o.warmupS());
        long stopAt = measureFrom + TimeUnit.SECONDS.toNanos(o.durationS());
        int buckets = o.durationS() * 10;
        AtomicBoolean running = new AtomicBoolean(true);
        List<Recorder> recorders = new ArrayList<>();
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        for (int w = 0; w < o.concurrency(); w++) {
            Recorder rec = new Recorder(buckets);
            recorders.add(rec);
            int workerId = w;
            workers.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                int rr = workerId;
                while (running.get()) {
                    boolean write = random.nextDouble() >= o.readRatio();
                    String key = "bench-" + random.nextInt(o.readRatio() > 0 ? Math.min(o.keys(), 2_000) : o.keys());
                    String base = o.spread() ? o.targets().get(rr++ % o.targets().size())
                            : write || !o.consistency().equals("stale") ? leader.get()
                            : o.targets().get(rr++ % o.targets().size());
                    HttpRequest.Builder b = write
                            ? HttpRequest.newBuilder(URI.create(base + "/api/v1/kv/" + key))
                                .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(body))
                            : HttpRequest.newBuilder(URI.create(base + "/api/v1/kv/" + key + "?consistency=" + o.consistency())).GET();
                    long t0 = System.nanoTime();
                    if (t0 >= stopAt) break;
                    int status;
                    String error;
                    try {
                        HttpResponse<String> r = http.send(b.timeout(Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.ofString());
                        status = r.statusCode();
                        error = status >= 400 && !(status == 404 && !write) ? errorCode(r.body(), status) : null;
                    } catch (IOException e) {
                        status = -1;
                        error = e.getClass().getSimpleName();
                    } catch (InterruptedException e) {
                        return;
                    }
                    long t1 = System.nanoTime();
                    if (t0 < measureFrom) continue;
                    int bucket = (int) ((t1 - measureFrom) / 100_000_000L);
                    if (error == null) {
                        rec.ok(write, (t1 - t0) / 1000, bucket);
                    } else {
                        rec.error(write, error, bucket);
                        if (!o.spread()) {
                            String found = discoverLeader(http, o.targets());
                            if (found != null) leader.set(found);
                        }
                    }
                }
            });
        }
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(stopAt - System.nanoTime()) + 50);
        running.set(false);
        workers.shutdown();
        workers.awaitTermination(15, TimeUnit.SECONDS);

        String json = report(o, recorders);
        System.out.println(json);
        if (o.out() != null) Files.writeString(o.out(), json);
        System.exit(0);
    }

    private static String report(Options o, List<Recorder> recorders) {
        int w = 0, r = 0;
        for (Recorder rec : recorders) {
            w += rec.writes;
            r += rec.reads;
        }
        long[] writes = new long[w];
        long[] reads = new long[r];
        int wi = 0, ri = 0;
        long writeErrors = 0, readErrors = 0;
        Map<String, Long> codes = new LinkedHashMap<>();
        long[] ok = new long[o.durationS() * 10];
        long[] err = new long[o.durationS() * 10];
        for (Recorder rec : recorders) {
            System.arraycopy(rec.writeLatencyMicros, 0, writes, wi, rec.writes);
            wi += rec.writes;
            System.arraycopy(rec.readLatencyMicros, 0, reads, ri, rec.reads);
            ri += rec.reads;
            writeErrors += rec.writeErrors;
            readErrors += rec.readErrors;
            rec.errorCodes.forEach((k, v) -> codes.merge(k, v, Long::sum));
            for (int i = 0; i < ok.length; i++) {
                ok[i] += rec.okPerBucket[i];
                err[i] += rec.errPerBucket[i];
            }
        }
        Arrays.sort(writes);
        Arrays.sort(reads);
        // Longest stretch of consecutive 100 ms buckets without a single successful request.
        int longestGap = 0, gap = 0;
        for (long n : ok) {
            gap = n == 0 ? gap + 1 : 0;
            longestGap = Math.max(longestGap, gap);
        }
        StringBuilder sb = new StringBuilder("{");
        field(sb, "label", '"' + o.label() + '"');
        field(sb, "timestamp", '"' + Instant.now().toString() + '"');
        field(sb, "targets", o.targets().size());
        field(sb, "concurrency", o.concurrency());
        field(sb, "durationSeconds", o.durationS());
        field(sb, "warmupSeconds", o.warmupS());
        field(sb, "readRatio", o.readRatio());
        field(sb, "valueBytes", o.valueBytes());
        field(sb, "keys", o.keys());
        field(sb, "consistency", '"' + o.consistency() + '"');
        field(sb, "spread", o.spread());
        field(sb, "writes", stats(writes, writeErrors, o.durationS()));
        field(sb, "reads", stats(reads, readErrors, o.durationS()));
        field(sb, "totalOpsPerSecond", fmt((writes.length + reads.length) / (double) o.durationS()));
        field(sb, "longestUnavailableMs", longestGap * 100);
        StringBuilder c = new StringBuilder("{");
        codes.forEach((k, v) -> c.append(c.length() > 1 ? "," : "").append('"').append(k).append("\":").append(v));
        field(sb, "errors", c.append('}').toString());
        if (o.timeline()) {
            StringBuilder t = new StringBuilder("[");
            for (int i = 0; i < ok.length; i++) t.append(i == 0 ? "" : ",").append('[').append(ok[i]).append(',').append(err[i]).append(']');
            field(sb, "timeline100ms", t.append(']').toString());
        }
        sb.setLength(sb.length() - 1);
        return sb.append('}').toString();
    }

    private static String stats(long[] sortedMicros, long errors, int seconds) {
        return "{\"ok\":" + sortedMicros.length + ",\"errors\":" + errors + ",\"opsPerSecond\":"
                + fmt(sortedMicros.length / (double) seconds) + ",\"p50Ms\":" + pct(sortedMicros, 0.50) + ",\"p95Ms\":"
                + pct(sortedMicros, 0.95) + ",\"p99Ms\":" + pct(sortedMicros, 0.99) + ",\"maxMs\":"
                + (sortedMicros.length == 0 ? "0" : fmt(sortedMicros[sortedMicros.length - 1] / 1000.0)) + "}";
    }

    private static String pct(long[] sorted, double q) {
        if (sorted.length == 0) return "0";
        int rank = (int) Math.ceil(q * sorted.length) - 1;
        return fmt(sorted[Math.max(0, Math.min(sorted.length - 1, rank))] / 1000.0);
    }

    private static String fmt(double v) {
        return String.format(Locale.ROOT, "%.3f", v);
    }

    private static void field(StringBuilder sb, String name, Object value) {
        sb.append('"').append(name).append("\":").append(value).append(',');
    }

    private static String errorCode(String body, int status) {
        int i = body.indexOf("\"error\":\"");
        if (i < 0) return "HTTP_" + status;
        int j = body.indexOf('"', i + 9);
        return j < 0 ? "HTTP_" + status : body.substring(i + 9, j);
    }

    /**
     * The target URL of the current leader. The leader is identified by node id and matched against the
     * targets' own ids: a node's advertised URL can be unreachable from the client (in Docker it is the
     * container-internal name, e.g. http://node1:8080). The advertised URL is used only if no target matches.
     */
    private static String discoverLeader(HttpClient http, List<String> targets) {
        for (String t : targets) {
            String body = get(http, t + "/api/v1/leader");
            if (body == null) continue;
            String leaderId = jsonString(body, "leaderId");
            if (leaderId == null) continue;
            for (String candidate : targets) {
                String health = get(http, candidate + "/api/v1/health");
                if (health != null && leaderId.equals(jsonString(health, "node"))) return candidate;
            }
            String advertised = jsonString(body, "httpUrl");
            if (advertised != null) return advertised;
        }
        return targets.get(0);
    }

    /** GET returning the body on HTTP 200, or null. */
    private static String get(HttpClient http, String url) {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMillis(800)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 200 ? r.body() : null;
        } catch (IOException | IllegalArgumentException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static String jsonString(String body, String field) {
        String key = "\"" + field + "\":\"";
        int i = body.indexOf(key);
        if (i < 0) return null;
        int j = body.indexOf('"', i + key.length());
        return j < 0 ? null : body.substring(i + key.length(), j);
    }

    private static void send(HttpClient http, HttpRequest.Builder b) throws IOException, InterruptedException {
        http.send(b.timeout(Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.discarding());
    }

    private static Options parse(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("unexpected argument " + args[i]);
            String k = args[i].substring(2);
            boolean flag = k.equals("spread") || k.equals("timeline");
            m.put(k, flag ? "true" : args[++i]);
        }
        String targets = m.getOrDefault("targets", "http://localhost:8081");
        return new Options(List.of(targets.split(",")), Integer.parseInt(m.getOrDefault("concurrency", "16")),
                Integer.parseInt(m.getOrDefault("duration", "20")), Integer.parseInt(m.getOrDefault("warmup", "5")),
                Double.parseDouble(m.getOrDefault("read-ratio", "0")), Integer.parseInt(m.getOrDefault("value-bytes", "128")),
                Integer.parseInt(m.getOrDefault("keys", "10000")), m.getOrDefault("consistency", "linearizable"),
                m.containsKey("spread"), m.containsKey("timeline"), m.getOrDefault("label", "run"),
                m.containsKey("out") ? Path.of(m.get("out")) : null);
    }
}
