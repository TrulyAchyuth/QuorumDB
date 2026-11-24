package io.quorumdb.server;

import io.quorumdb.server.ProcessCluster.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests against real QuorumDB processes communicating over gRPC on localhost.
 * Every measurement printed here (prefixed EVIDENCE) is a real wall-clock measurement on the test
 * machine, not a simulation result.
 */
@Tag("integration")
class RealClusterIT {
    @TempDir
    Path dir;
    private ProcessCluster cluster;

    @AfterEach
    void tearDown(TestInfo info) {
        if (cluster != null) cluster.close(); // SIGKILLs anything still running
        evidence(info.getDisplayName() + " finished; all node processes terminated");
    }

    private static void evidence(String line) {
        System.out.println("EVIDENCE " + line);
    }

    private String other(String notThis) {
        return cluster.ids().stream().filter(n -> !n.equals(notThis)).findFirst().orElseThrow();
    }

    @Test
    void freshClusterElectsOneLeaderAndServesRequestsThroughAnyNode() throws Exception {
        long t0 = System.nanoTime();
        cluster = ProcessCluster.start(dir, 3);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        evidence("3 processes started and elected " + leader + " in " + (System.nanoTime() - t0) / 1_000_000 + " ms (incl. JVM startup)");

        // Every node agrees on the leader, and exactly one claims the role.
        JsonNode nodes = cluster.get("n1", "/api/v1/nodes").body();
        int leaders = 0;
        for (JsonNode n : nodes) {
            assertThat(n.get("reachable").asBoolean()).isTrue();
            if ("LEADER".equals(n.get("role").asString())) leaders++;
            assertThat(n.get("leaderId").asString()).isEqualTo(leader);
        }
        assertThat(leaders).isEqualTo(1);

        String follower = other(leader);
        Response put = cluster.put(follower, "greeting", "hello");
        assertThat(put.status()).as(put.raw()).isEqualTo(201);
        assertThat(put.body().get("servedBy").asString()).as("follower forwarded the write to the leader").isEqualTo(leader);
        for (String n : cluster.ids()) {
            Response get = cluster.get(n, "/api/v1/kv/greeting");
            assertThat(get.status()).isEqualTo(200);
            assertThat(get.body().get("value").asString()).isEqualTo("hello");
        }
    }

    @Test
    void nodeIsAddedToARunningClusterAndTheLeaderIsRemoved() throws Exception {
        cluster = ProcessCluster.start(dir, 4, 3);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        assertThat(cluster.put(leader, "before", "1").status()).isEqualTo(201);

        cluster.startNode("n4");
        cluster.awaitHttp("n4");
        Response add = cluster.admin(leader, "POST", "/api/v1/members", cluster.memberBody("n4"));
        assertThat(add.status()).as(add.raw()).isEqualTo(200);
        ProcessCluster.await(Duration.ofSeconds(10), () -> {
            Response r = cluster.get("n4", "/api/v1/kv/before?consistency=stale");
            return r.status() == 200 && "1".equals(r.body().get("value").asString());
        }, "n4 never caught up");
        String other = other(leader);
        ProcessCluster.await(Duration.ofSeconds(5),
                () -> cluster.get(other, "/api/v1/members").raw().contains("n4="), other + " never learned of n4");
        evidence("n4 added to a running 3-node cluster and caught up");

        Response remove = cluster.admin(leader, "DELETE", "/api/v1/members/" + leader, null);
        assertThat(remove.status()).as(remove.raw()).isEqualTo(200);
        String[] next = new String[1];
        ProcessCluster.await(Duration.ofSeconds(10), () -> {
            next[0] = cluster.leader("n4");
            return next[0] != null && !next[0].equals(leader);
        }, "no new leader after removing " + leader);
        assertThat(cluster.put(next[0], "after", "2").status()).isEqualTo(201);
        ProcessCluster.await(Duration.ofSeconds(10), () -> {
            Response r = cluster.get("n4", "/api/v1/kv/after?consistency=stale");
            return r.status() == 200 && "2".equals(r.body().get("value").asString());
        }, "n4 did not receive writes from the new leader");
        assertThat(cluster.get(next[0], "/api/v1/members").raw()).doesNotContain("\"" + leader + "\"")
                .doesNotContain("\"" + leader + "=");
        evidence(leader + " removed; " + next[0] + " leads {the other two, n4}");
    }

    @Test
    void leaderSigkillFailsOverWithoutLosingAcknowledgedWritesAndTheNodeCatchesUp() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        Map<String, String> acknowledged = new LinkedHashMap<>();
        for (int i = 0; i < 50; i++) {
            Response r = cluster.put(other(leader), "pre-" + i, "v" + i);
            assertThat(r.status()).as(r.raw()).isIn(200, 201);
            acknowledged.put("pre-" + i, "v" + i);
        }

        // A writer keeps writing through the surviving nodes while the leader dies.
        List<String> survivors = cluster.ids().stream().filter(n -> !n.equals(leader)).toList();
        AtomicBoolean writing = new AtomicBoolean(true);
        Map<String, String> acknowledgedDuring = new java.util.concurrent.ConcurrentHashMap<>();
        int[] failedDuring = new int[1];
        ExecutorService writer = Executors.newSingleThreadExecutor();
        writer.submit(() -> {
            int i = 0;
            while (writing.get()) {
                String key = "during-" + i;
                Response r = cluster.put(survivors.get(i % 2), key, "d" + i);
                if (r.status() == 200 || r.status() == 201) acknowledgedDuring.put(key, "d" + i); else failedDuring[0]++;
                i++;
            }
        });
        Thread.sleep(300);
        long killedAt = System.nanoTime();
        cluster.kill(leader);
        String newLeader = cluster.awaitLeader(survivors.get(0), Duration.ofSeconds(10));
        long failoverMs = (System.nanoTime() - killedAt) / 1_000_000;
        assertThat(newLeader).isNotEqualTo(leader);
        Thread.sleep(1_000);
        writing.set(false);
        writer.shutdown();
        writer.awaitTermination(15, java.util.concurrent.TimeUnit.SECONDS);
        evidence("SIGKILL of leader " + leader + ": new leader " + newLeader + " observed after " + failoverMs
                + " ms (HTTP polling every 25 ms, so an upper bound); writes during failover: "
                + acknowledgedDuring.size() + " acknowledged, " + failedDuring[0] + " rejected");
        assertThat(failoverMs).as("failover must take about one election timeout, not seconds").isLessThan(3_000);
        assertThat(acknowledgedDuring).as("writes resume on the surviving majority").isNotEmpty();

        // No acknowledged write was lost.
        acknowledged.putAll(acknowledgedDuring);
        for (Map.Entry<String, String> e : acknowledged.entrySet()) {
            Response r = cluster.get(survivors.get(0), "/api/v1/kv/" + e.getKey());
            assertThat(r.status()).as(e.getKey()).isEqualTo(200);
            assertThat(r.body().get("value").asString()).isEqualTo(e.getValue());
        }

        // Restart the killed process: it recovers from its WAL and catches up from the leader.
        long restartAt = System.nanoTime();
        cluster.restart(leader);
        ProcessCluster.await(Duration.ofSeconds(20), () -> {
            Response me = cluster.get(leader, "/api/v1/node");
            Response lead = cluster.get(newLeader, "/api/v1/node");
            return me.status() == 200 && lead.status() == 200
                    && me.body().get("lastApplied").asLong() == lead.body().get("commitIndex").asLong()
                    && "FOLLOWER".equals(me.body().get("role").asString());
        }, "restarted node did not catch up");
        evidence("restarted " + leader + " recovered from disk and caught up in " + (System.nanoTime() - restartAt) / 1_000_000
                + " ms (incl. JVM startup); verified " + acknowledged.size() + " acknowledged keys on it");
        for (Map.Entry<String, String> e : acknowledged.entrySet()) {
            Response r = cluster.get(leader, "/api/v1/kv/" + e.getKey() + "?consistency=stale");
            assertThat(r.status()).as(e.getKey()).isEqualTo(200);
            assertThat(r.body().get("value").asString()).isEqualTo(e.getValue());
        }
    }

    @Test
    void isolatedLeaderCannotCommitAndMajoritySideKeepsServing() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        String oldLeader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        assertThat(cluster.put(oldLeader, "before", "1").status()).isEqualTo(201);
        long commitBefore = cluster.get(oldLeader, "/api/v1/node").body().get("commitIndex").asLong();

        Response fault = cluster.admin(oldLeader, "POST", "/api/v1/faults", Map.of("type", "ISOLATE", "targets", List.of(oldLeader)));
        assertThat(fault.status()).as(fault.raw()).isEqualTo(201);

        // A write sent to the isolated (old) leader must never be acknowledged.
        Response minorityWrite = cluster.put(oldLeader, "minority", "must-not-commit");
        assertThat(minorityWrite.status()).as(minorityWrite.raw()).isNotIn(200, 201);
        evidence("write to isolated leader " + oldLeader + " answered HTTP " + minorityWrite.status() + " "
                + minorityWrite.body().get("error").asString());

        String majorityNode = other(oldLeader);
        String[] found = new String[1];
        ProcessCluster.await(Duration.ofSeconds(10), () -> {
            found[0] = cluster.leader(majorityNode);
            return found[0] != null && !found[0].equals(oldLeader);
        }, "majority side did not elect a new leader");
        String newLeader = found[0];
        Response majorityWrite = cluster.put(majorityNode, "majority", "ok");
        assertThat(majorityWrite.status()).as(majorityWrite.raw()).isEqualTo(201);

        JsonNode isolated = cluster.get(oldLeader, "/api/v1/node").body();
        assertThat(isolated.get("commitIndex").asLong()).as("minority made no commit progress").isEqualTo(commitBefore);
        assertThat(isolated.get("role").asString()).as("check-quorum deposed the isolated leader").isNotEqualTo("LEADER");
        evidence("isolated " + oldLeader + ": commit index stayed " + commitBefore + ", role " + isolated.get("role").asString()
                + "; majority elected " + newLeader + " and committed a write");

        assertThat(cluster.admin(oldLeader, "DELETE", "/api/v1/faults", null).status()).isEqualTo(200);
        ProcessCluster.await(Duration.ofSeconds(10), () -> {
            Response r = cluster.get(oldLeader, "/api/v1/kv/majority?consistency=stale");
            return r.status() == 200;
        }, "old leader did not rejoin");
        for (String n : cluster.ids()) {
            assertThat(cluster.get(n, "/api/v1/kv/minority?consistency=stale").status())
                    .as("the unacknowledged minority write is gone on " + n).isEqualTo(404);
            assertThat(cluster.get(n, "/api/v1/kv/before?consistency=stale").status()).isEqualTo(200);
            JsonNode faults = cluster.get(n, "/api/v1/node").body().get("faults");
            assertThat(faults.get("blockedPeers")).as("faults cleaned up on " + n).isEmpty();
        }
    }

    @Test
    void fiveNodeClusterSplitTwoThreeOnlyMajorityCommits() throws Exception {
        cluster = ProcessCluster.start(dir, 5);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        List<String> others = cluster.ids().stream().filter(n -> !n.equals(leader)).toList();
        List<String> minority = List.of(leader, others.get(0));
        List<String> majority = others.subList(1, 4);
        Response fault = cluster.admin("n1", "POST", "/api/v1/faults",
                Map.of("type", "PARTITION", "groups", List.of(minority, majority), "durationMs", 6000));
        assertThat(fault.status()).as(fault.raw()).isEqualTo(201);
        long commitAtSplit = cluster.get(leader, "/api/v1/node").body().get("commitIndex").asLong();

        // The management plane (HTTP) is not partitioned, so right after the split the cluster view still
        // shows the old leader until check-quorum makes it step down. Wait for the majority's own leader.
        String[] found = new String[1];
        ProcessCluster.await(Duration.ofSeconds(10), () -> majority.contains(found[0] = cluster.leader(majority.get(0))),
                "majority side did not elect a leader");
        String newLeader = found[0];
        assertThat(majority).contains(newLeader);
        assertThat(cluster.put(majority.get(1), "maj", "x").status()).isEqualTo(201);
        assertThat(cluster.put(minority.get(1), "min", "y").status()).isNotIn(200, 201);
        for (String n : minority) {
            assertThat(cluster.get(n, "/api/v1/node").body().get("commitIndex").asLong()).as(n).isEqualTo(commitAtSplit);
        }
        evidence("5-node split " + minority + " | " + majority + ": minority commit stayed " + commitAtSplit
                + ", majority elected " + newLeader + " and committed");

        // The partition has a 6 s duration: every node heals it on its own.
        ProcessCluster.await(Duration.ofSeconds(15), () -> cluster.ids().stream().allMatch(n -> {
            Response r = cluster.get(n, "/api/v1/kv/maj?consistency=stale");
            return r.status() == 200;
        }), "partition did not heal by itself");
        for (String n : cluster.ids()) {
            assertThat(cluster.get(n, "/api/v1/kv/min?consistency=stale").status()).isEqualTo(404);
            assertThat(cluster.get(n, "/api/v1/node").body().get("faults").get("active")).isEmpty();
        }
    }

    @Test
    void fullClusterSigkillAndRestartRecoversAllDataFromDisk() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        for (int i = 0; i < 100; i++) assertThat(cluster.put(leader, "k" + i, "value-" + i).status()).isEqualTo(201);
        long term = cluster.get(leader, "/api/v1/node").body().get("term").asLong();

        for (String n : cluster.ids()) cluster.kill(n);
        assertThat(cluster.running()).isEmpty();
        for (String n : cluster.ids()) cluster.startNode(n);
        for (String n : cluster.ids()) cluster.awaitHttp(n);
        String newLeader = cluster.awaitLeader("n1", Duration.ofSeconds(15));
        long newTerm = cluster.get(newLeader, "/api/v1/node").body().get("term").asLong();
        assertThat(newTerm).as("terms were persisted").isGreaterThan(term);
        for (int i = 0; i < 100; i++) {
            Response r = cluster.get("n" + (1 + i % 3), "/api/v1/kv/k" + i);
            assertThat(r.status()).isEqualTo(200);
            assertThat(r.body().get("value").asString()).isEqualTo("value-" + i);
        }
        evidence("all 3 processes SIGKILLed and restarted: 100/100 acknowledged keys recovered, term " + term + " -> " + newTerm);
        assertThat(Files.exists(dir.resolve("data/n1/wal"))).isTrue();
    }

    @Test
    void liveFaultLabExperimentIsAuditedAndLeavesNoFaultsBehind() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        cluster.awaitLeader("n1", Duration.ofSeconds(10));
        Map<String, Object> scenario = Map.of("name", "live leader crash", "durationMs", 8000,
                "workload", Map.of("clients", 3, "opIntervalMs", 30, "keys", 4, "readRatio", 0.5, "deleteRatio", 0.05),
                "faults", List.of(
                        Map.of("type", "CRASH", "targets", List.of("leader"), "start", Map.of("atMs", 2000), "durationMs", 2000),
                        Map.of("type", "ISOLATE", "targets", List.of("follower"), "start", Map.of("atMs", 5000), "durationMs", 1500)));
        Response started = cluster.admin("n1", "POST", "/api/v1/faultlab/live-runs", Map.of("scenario", scenario));
        assertThat(started.status()).as(started.raw()).isEqualTo(202);
        String[] runId = new String[1];
        ProcessCluster.await(Duration.ofSeconds(40), () -> {
            JsonNode s = cluster.get("n1", "/api/v1/faultlab/live-runs/current").body();
            if ("FAILED".equals(s.get("state").asString())) throw new AssertionError("live run failed: " + s);
            if ("COMPLETE".equals(s.get("state").asString())) runId[0] = s.get("runId").asString();
            return runId[0] != null;
        }, "live run did not complete");

        JsonNode run = cluster.get("n1", "/api/v1/faultlab/runs/" + runId[0]).body();
        JsonNode summary = run.get("summary");
        evidence("live FaultLab run " + runId[0] + ": headline=" + summary.get("headline").asString() + " safety="
                + summary.get("safety").asString() + " linearizability=" + summary.get("linearizability").asString()
                + " ops=" + summary.get("opsTotal").asLong() + " elections=" + summary.get("elections").asInt()
                + " events=" + summary.get("events").asInt());
        assertThat(summary.get("mode").asString()).isEqualTo("LIVE");
        assertThat(summary.get("safety").asString()).as(run.get("safety").toString()).isIn("SAFE", "INCONCLUSIVE");
        assertThat(summary.get("safety").asString()).as("gaps: " + run.get("safety").get("observationGaps")).isEqualTo("SAFE");
        assertThat(summary.get("linearizability").asString()).isEqualTo("LINEARIZABLE");
        assertThat(summary.get("headline").asString()).isIn("HEALTHY", "EXPECTED_UNAVAILABILITY");
        assertThat(run.get("faults")).hasSize(2);
        assertThat(run.get("faults").get(0).get("hitLeader").asBoolean()).isTrue();
        assertThat(summary.get("opsTotal").asLong()).isGreaterThan(100);
        List<String> checked = new ArrayList<>();
        for (JsonNode c : run.get("safety").get("checks")) {
            if (c.get("status").asString().equals("PASS")) checked.add(c.get("id").asString());
        }
        assertThat(checked).contains("ELECTION_SAFETY", "COMMIT_AGREEMENT", "LOG_MATCHING", "STATE_MACHINE_SAFETY");

        // Cleanup: no fault remains anywhere and every node serves again.
        for (String n : cluster.ids()) {
            JsonNode node = cluster.get(n, "/api/v1/node").body();
            assertThat(node.get("health").asString()).as(n).isEqualTo("UP");
            assertThat(node.get("faults").get("active")).as(n).isEmpty();
            assertThat(node.get("faults").get("blockedPeers")).as(n).isEmpty();
            assertThat(node.get("faults").get("crashed").asBoolean()).isFalse();
        }
        // Replaying a live run re-executes its scenario in the simulator (and says so).
        Response replay = cluster.get("n1", "/api/v1/faultlab/runs/" + runId[0]);
        assertThat(replay.status()).isEqualTo(200);
        JsonNode r = cluster.admin("n1", "POST", "/api/v1/faultlab/runs/" + runId[0] + "/replay", null).body();
        assertThat(r.get("mode").asString()).isEqualTo("LIVE");
        assertThat(r.get("identical").asBoolean()).isFalse();
        assertThat(r.get("note").asString()).contains("simulator");
    }

    /**
     * Regression: the live audit samples each node's durable log in windows. With more than 5,000 entries
     * after the last snapshot, a node that crashed and restarted was sampled only up to the first page while
     * reporting a much higher commit index, and the checker reported committed entries as "missing"
     * (a false Commit Durability violation). The audit must see the whole log and report SAFE.
     */
    @Test
    void liveAuditOfALongLogAcrossACrashReportsNoFalseViolation() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        String leader = cluster.awaitLeader("n1", Duration.ofSeconds(10));
        int preload = 6_500;
        ExecutorService pool = Executors.newFixedThreadPool(32);
        java.util.concurrent.atomic.AtomicInteger ok = new java.util.concurrent.atomic.AtomicInteger();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 32; t++) {
            int worker = t;
            futures.add(pool.submit(() -> {
                for (int i = worker; i < preload; i += 32) {
                    int st = cluster.put(leader, "pre-" + (i % 500), "v" + i).status();
                    if (st == 200 || st == 201) ok.incrementAndGet();
                }
            }));
        }
        for (var f : futures) f.get();
        pool.shutdown();
        assertThat(ok.get()).isEqualTo(preload);
        long commit = cluster.get(leader, "/api/v1/node").body().get("commitIndex").asLong();
        evidence("preloaded " + preload + " writes, leader commit index " + commit);
        assertThat(commit).as("log longer than one 5,000-entry sampling page").isGreaterThan(5_000);

        Map<String, Object> scenario = Map.of("name", "long log, follower crash", "durationMs", 6000,
                "workload", Map.of("clients", 2, "opIntervalMs", 50, "keys", 4, "readRatio", 0.5, "deleteRatio", 0.0),
                "faults", List.of(Map.of("type", "CRASH", "targets", List.of("follower"), "start", Map.of("atMs", 1000),
                        "durationMs", 2500)));
        Response started = cluster.admin("n1", "POST", "/api/v1/faultlab/live-runs", Map.of("scenario", scenario));
        assertThat(started.status()).as(started.raw()).isEqualTo(202);
        String[] runId = new String[1];
        ProcessCluster.await(Duration.ofSeconds(40), () -> {
            JsonNode s = cluster.get("n1", "/api/v1/faultlab/live-runs/current").body();
            if ("FAILED".equals(s.get("state").asString())) throw new AssertionError("live run failed: " + s);
            if ("COMPLETE".equals(s.get("state").asString())) runId[0] = s.get("runId").asString();
            return runId[0] != null;
        }, "live run did not complete");
        JsonNode run = cluster.get("n1", "/api/v1/faultlab/runs/" + runId[0]).body();
        evidence("long-log live run " + runId[0] + ": safety=" + run.get("summary").get("safety").asString());
        assertThat(run.get("summary").get("safety").asString()).as(run.get("safety").toString()).isEqualTo("SAFE");
        for (JsonNode c : run.get("safety").get("checks")) {
            if (c.get("id").asString().equals("COMMIT_DURABILITY")) {
                // The checker knows the entries committed during the run (beyond index 5,000 here), and before the
                // fix exactly those were reported missing from the restarted node's truncated sample.
                assertThat(c.get("examined").asLong()).as("durability was actually checked").isGreaterThan(0);
                assertThat(c.get("violations")).isEmpty();
            }
        }
    }

    /**
     * Regression: simulated runs are open to any client and were bounded only by scenario validation, which
     * allows traces of tens of millions of events. Such a request must be refused with 422 before it exhausts
     * the node's heap, and the node must keep serving, including normal simulations.
     */
    @Test
    void oversizedSimulationIsRefusedAndTheNodeStaysHealthy() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        cluster.awaitLeader("n1", Duration.ofSeconds(10));
        Map<String, Object> hostile = Map.of("name", "too large", "nodes", 9, "durationMs", 600_000,
                "cluster", Map.of("electionTimeoutMinMs", 10, "electionTimeoutMaxMs", 20, "heartbeatIntervalMs", 5),
                "workload", Map.of("clients", 32, "opIntervalMs", 1, "keys", 1000, "readRatio", 0.5, "deleteRatio", 0.0));
        long t0 = System.nanoTime();
        Response r = cluster.post("n1", "/api/v1/faultlab/runs", Map.of("scenario", hostile));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        evidence("oversized simulation answered HTTP " + r.status() + " in " + ms + " ms: " + r.raw());
        assertThat(r.status()).as(r.raw()).isEqualTo(422);
        assertThat(r.body().get("error").asString()).isEqualTo("RUN_TOO_LARGE");

        // Still healthy, still a member of the cluster, and normal simulations still run.
        assertThat(cluster.get("n1", "/api/v1/health").body().get("status").asString()).isEqualTo("UP");
        assertThat(cluster.put("n1", "after-oversized", "ok").status()).isIn(200, 201);
        Response normal = cluster.post("n1", "/api/v1/faultlab/runs", Map.of("preset", "leader-crash"));
        assertThat(normal.status()).as(normal.raw()).isEqualTo(201);
    }

    /**
     * Regression: every live run used the same key names, and the linearizability model starts each key
     * empty. A second run on the same cluster read the first run's values and was reported NOT_LINEARIZABLE
     * ("returned a value that no client ever wrote") although the database behaved correctly.
     */
    @Test
    void consecutiveLiveRunsOnTheSameClusterAreEachLinearizable() throws Exception {
        cluster = ProcessCluster.start(dir, 3);
        cluster.awaitLeader("n1", Duration.ofSeconds(10));
        Map<String, Object> scenario = Map.of("name", "back to back", "durationMs", 3000,
                "workload", Map.of("clients", 3, "opIntervalMs", 20, "keys", 3, "readRatio", 0.5, "deleteRatio", 0.0),
                "faults", List.of());
        String previous = "";
        for (int run = 1; run <= 2; run++) {
            Response started = cluster.admin("n1", "POST", "/api/v1/faultlab/live-runs", Map.of("scenario", scenario));
            assertThat(started.status()).as(started.raw()).isEqualTo(202);
            String[] runId = new String[1];
            String before = previous;
            ProcessCluster.await(Duration.ofSeconds(30), () -> {
                JsonNode s = cluster.get("n1", "/api/v1/faultlab/live-runs/current").body();
                if ("FAILED".equals(s.get("state").asString())) throw new AssertionError("live run failed: " + s);
                JsonNode id = s.get("runId");
                if ("COMPLETE".equals(s.get("state").asString()) && id != null && !id.isNull() && !id.asString().equals(before)) {
                    runId[0] = id.asString(); // a completed run that is not the previous one
                }
                return runId[0] != null;
            }, "live run " + run + " did not complete");
            previous = runId[0];
            JsonNode r = cluster.get("n1", "/api/v1/faultlab/runs/" + runId[0]).body();
            evidence("back-to-back live run " + run + " (" + runId[0] + "): linearizability="
                    + r.get("summary").get("linearizability").asString() + " ops=" + r.get("summary").get("opsTotal").asLong());
            assertThat(r.get("summary").get("linearizability").asString()).as(r.get("linearizability").toString())
                    .isEqualTo("LINEARIZABLE");
            assertThat(r.get("summary").get("opsTotal").asLong()).isGreaterThan(50);
        }
    }
}
