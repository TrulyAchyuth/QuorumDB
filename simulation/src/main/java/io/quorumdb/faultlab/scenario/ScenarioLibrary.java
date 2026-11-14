package io.quorumdb.faultlab.scenario;

import io.quorumdb.faultlab.scenario.Scenario.Cluster;
import io.quorumdb.faultlab.scenario.Scenario.Condition;
import io.quorumdb.faultlab.scenario.Scenario.FaultStep;
import io.quorumdb.faultlab.scenario.Scenario.FaultType;
import io.quorumdb.faultlab.scenario.Scenario.Trigger;
import io.quorumdb.faultlab.scenario.Scenario.Workload;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Built-in scenarios. Each exercises one well-known Raft failure mode. */
public final class ScenarioLibrary {
    private ScenarioLibrary() {}

    public static Map<String, Scenario> all() {
        Map<String, Scenario> s = new LinkedHashMap<>();
        s.put("leader-crash", new Scenario("Leader crash",
                "Kill the leader once the cluster is serving writes; restart it 3 s later. Expect a new leader within "
                        + "about one election timeout, no lost committed writes, and the old leader rejoining as a follower.",
                3, 7L, 10_000L, null, null,
                List.of(new FaultStep(FaultType.CRASH, List.of("leader"), null,
                        Trigger.when(Condition.COMMIT_INDEX_AT_LEAST, 20, 0), null, 3_000L, null, null))));

        s.put("follower-crash", new Scenario("Follower crash and catch-up",
                "Kill one follower for 3 s. The remaining majority keeps committing; the follower catches up from the "
                        + "leader's log after restart.",
                3, 11L, 10_000L, null, null,
                List.of(FaultStep.of(FaultType.CRASH, 2_000, 3_000L, "follower"))));

        s.put("leader-isolated", new Scenario("Leader isolated (minority partition)",
                "Cut the leader off from both followers. The isolated leader must not commit anything; the majority "
                        + "elects a new leader. After healing, the old leader discards its uncommitted entries.",
                3, 21L, 12_000L, null, null,
                List.of(FaultStep.of(FaultType.ISOLATE, 3_000, 4_000L, "leader"))));

        s.put("split-5", new Scenario("Five nodes split 2 / 3",
                "Partition a five-node cluster so the leader sits in the two-node minority. Only the three-node side "
                        + "can make progress.",
                5, 5L, 14_000L, null, null,
                List.of(new FaultStep(FaultType.PARTITION, List.of("leader", "follower"), null, Trigger.at(3_000), null,
                        5_000L, null, null))));

        s.put("no-quorum", new Scenario("Majority lost",
                "Crash two of three nodes. The survivor cannot form a quorum, so writes and linearizable reads must "
                        + "fail (expected unavailability, not a safety problem) until a second node returns.",
                3, 3L, 12_000L, null, null,
                List.of(FaultStep.of(FaultType.CRASH, 3_000, 4_000L, "follower"),
                        FaultStep.of(FaultType.CRASH, 3_200, 3_800L, "leader"))));

        s.put("flaky-network", new Scenario("Lossy, slow network",
                "Drop 25% of all messages involving two nodes and add 40 ms of latency to a third. Raft must tolerate "
                        + "loss, delay, duplication and reordering without violating safety.",
                5, 13L, 12_000L, null, null,
                List.of(FaultStep.of(FaultType.DROP, 2_000, 7_000L, "n1", "n2").withDrop(0.25),
                        FaultStep.of(FaultType.DELAY, 2_000, 7_000L, "n3").withDelay(40))));

        s.put("snapshot-catch-up", new Scenario("Lagging follower restored from snapshot",
                "Snapshot every 50 entries. A follower is down long enough that the leader compacts the entries it "
                        + "needs, so it must be brought back with InstallSnapshot.",
                3, 17L, 12_000L, new Cluster(null, null, null, null, null, 50L, null, null),
                new Workload(4, 15L, 8, 0.3, 0.05, null, null),
                List.of(FaultStep.of(FaultType.CRASH, 2_000, 5_000L, "follower"))));

        s.put("storage-failure", new Scenario("Leader disk failure",
                "The leader's disk starts rejecting writes. It must fail-stop instead of acknowledging writes it "
                        + "cannot persist; a new leader takes over. The node rejoins once its disk works again.",
                3, 19L, 12_000L, null, null,
                List.of(FaultStep.of(FaultType.STORAGE_FAILURE, 3_000, 4_000L, "leader"))));

        s.put("rolling-restart", new Scenario("Rolling restart",
                "Restart every node in turn, 1.5 s apart, under load, as in a rolling upgrade.",
                3, 23L, 10_000L, null, null,
                List.of(FaultStep.of(FaultType.RESTART, 2_000, null, "n1"),
                        FaultStep.of(FaultType.RESTART, 3_500, null, "n2"),
                        FaultStep.of(FaultType.RESTART, 5_000, null, "n3"))));

        s.put("chaos", new Scenario("Combined failures",
                "Overlapping faults on five nodes: an isolated leader, a crashed follower, a lossy link and a rolling "
                        + "partition. A stress test for the safety checker.",
                5, 29L, 20_000L, null, new Workload(5, 30L, 6, 0.5, 0.05, null, null),
                List.of(FaultStep.of(FaultType.ISOLATE, 2_000, 3_000L, "leader"),
                        FaultStep.of(FaultType.CRASH, 3_000, 4_000L, "follower"),
                        FaultStep.of(FaultType.DROP, 6_000, 5_000L, "random").withDrop(0.3),
                        new FaultStep(FaultType.PARTITION, null, List.of(List.of("n1", "n2"), List.of("n3", "n4", "n5")),
                                Trigger.at(9_000), null, 3_000L, null, null),
                        FaultStep.of(FaultType.RESTART, 13_000, null, "leader"),
                        FaultStep.of(FaultType.DELAY, 14_000, 3_000L, "random").withDelay(80))));

        Scenario rejoin = new Scenario("Disruptive rejoin (PreVote off)",
                "A follower is isolated for 5 s, then reconnected. Without PreVote it keeps starting elections while "
                        + "isolated, inflates its term, and deposes the healthy leader when it returns. Compare with "
                        + "the PreVote-on variant: same seed, no disruption.",
                3, 31L, 12_000L, new Cluster(null, null, null, false, false, null, null, null), null,
                List.of(FaultStep.of(FaultType.ISOLATE, 3_000, 5_000L, "follower")));
        s.put("rejoin-no-prevote", rejoin);
        s.put("rejoin-prevote", rejoin.withCluster(new Cluster(null, null, null, true, true, null, null, null))
                .withName("Quiet rejoin (PreVote on)"));

        s.put("disk-loss", new Scenario("Disk loss (broken assumption)",
                "Deliberately violates the assumption Raft's proof rests on: a node that acknowledged entries loses its "
                        + "disk. With the up-to-date leader down, the amnesiac node and a stale node form a majority and "
                        + "overwrite committed history. The safety checker is expected to report violations here.",
                3, 37L, 12_000L, null, new Workload(3, 30L, 4, 0.4, 0.0, null, null),
                List.of(new FaultStep(FaultType.ISOLATE, List.of("n3"), null, Trigger.at(1_500), null, 3_000L, null, null),
                        new FaultStep(FaultType.CRASH, List.of("n1"), null, Trigger.at(4_000), null, 4_000L, null, null),
                        new FaultStep(FaultType.DISK_LOSS, List.of("n2"), null, Trigger.at(4_100), null, 200L, null, null))));
        return s;
    }
}
