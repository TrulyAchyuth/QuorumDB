package io.quorumdb.faultlab;

import io.quorumdb.faultlab.sim.SimCluster;
import io.quorumdb.faultlab.sim.SimCluster.SimNode;
import io.quorumdb.faultlab.sim.SimOptions;
import io.quorumdb.kv.KvCommand;
import io.quorumdb.raft.Outcomes;
import io.quorumdb.raft.Outcomes.Status;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The central availability/safety trade-off, tested directly on a simulated cluster:
 * a minority can never commit, an acknowledged write is never lost, and state survives restarts.
 */
class MinorityPartitionTest {

    private static byte[] put(String key, String value) {
        return KvCommand.single("", 0, KvCommand.Op.put(key, value)).encode();
    }

    private static SimCluster started(int nodes, long seed) {
        SimCluster cluster = new SimCluster(SimOptions.defaults(nodes, seed));
        cluster.startAll();
        cluster.runFor(1_000);
        assertThat(cluster.leader()).as("a leader is elected within a second").isNotNull();
        return cluster;
    }

    @Test
    void exactlyOneLeaderIsElectedAndAllNodesAgreeOnIt() {
        for (long seed = 1; seed <= 20; seed++) {
            SimCluster cluster = started(5, seed);
            long leaders = 0;
            for (SimNode n : cluster.nodes()) if (n.raft().isLeader()) leaders++;
            assertThat(leaders).as("seed " + seed).isEqualTo(1);
            String leader = cluster.leader().id;
            for (SimNode n : cluster.nodes()) assertThat(n.raft().leaderId()).isEqualTo(leader);
        }
    }

    @Test
    void isolatedLeaderCannotCommitAndItsWritesAreNeverAcknowledged() {
        SimCluster cluster = started(3, 42);
        SimNode oldLeader = cluster.leader();
        List<Outcomes.Proposal> acked = new ArrayList<>();
        oldLeader.raft().propose(put("before", "1"), acked::add);
        cluster.runFor(200);
        assertThat(acked).singleElement().satisfies(o -> assertThat(o.status()).isEqualTo(Status.OK));
        long committedBefore = oldLeader.raft().commitIndex();

        cluster.network().isolate(oldLeader.id, true);
        List<Outcomes.Proposal> minorityResults = new ArrayList<>();
        for (int i = 0; i < 10; i++) oldLeader.raft().propose(put("minority-" + i, "x"), minorityResults::add);
        cluster.runFor(3_000);

        assertThat(oldLeader.raft().commitIndex()).as("minority side must not advance its commit index").isEqualTo(committedBefore);
        assertThat(oldLeader.kv().contains("minority-0")).isFalse();
        assertThat(minorityResults).hasSize(10)
                .allSatisfy(o -> assertThat(o.status()).as("never reported as success").isNotEqualTo(Status.OK));

        SimNode newLeader = cluster.leader();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader.id).isNotEqualTo(oldLeader.id);
        assertThat(newLeader.raft().currentTerm()).isGreaterThan(1);
        List<Outcomes.Proposal> majorityResults = new ArrayList<>();
        newLeader.raft().propose(put("majority", "y"), majorityResults::add);
        cluster.runFor(200);
        assertThat(majorityResults).singleElement().satisfies(o -> assertThat(o.status()).isEqualTo(Status.OK));

        cluster.network().healAll();
        cluster.runFor(2_000);
        for (SimNode n : cluster.nodes()) {
            assertThat(n.kv().get("before")).as("acknowledged write survives on " + n.id).isNotNull();
            assertThat(n.kv().get("majority")).isNotNull();
            assertThat(n.kv().contains("minority-0")).as("unacknowledged minority write is gone on " + n.id).isFalse();
            assertThat(n.kv().digest()).isEqualTo(newLeader.kv().digest());
        }
    }

    @Test
    void twoOfFiveCannotCommitThreeOfFiveCan() {
        SimCluster cluster = started(5, 7);
        String leader = cluster.leader().id;
        List<String> others = cluster.nodeIds().stream().filter(n -> !n.equals(leader)).toList();
        List<String> minority = List.of(leader, others.get(0));
        List<String> majority = others.subList(1, 4);
        cluster.network().partition(List.of(minority, majority));
        long commitAtSplit = cluster.node(leader).raft().commitIndex();
        cluster.node(leader).raft().propose(put("lost", "x"), r -> { });
        cluster.runFor(3_000);

        for (String id : minority) {
            assertThat(cluster.node(id).raft().commitIndex()).as(id).isEqualTo(commitAtSplit);
            assertThat(cluster.node(id).raft().isLeader()).as("check-quorum deposed the minority leader").isFalse();
        }
        SimNode majorityLeader = cluster.leader();
        assertThat(majority).contains(majorityLeader.id);
        List<Outcomes.Proposal> results = new ArrayList<>();
        majorityLeader.raft().propose(put("kept", "y"), results::add);
        cluster.runFor(300);
        assertThat(results.get(0).status()).isEqualTo(Status.OK);
    }

    @Test
    void committedDataSurvivesFullClusterRestart() {
        SimCluster cluster = started(3, 11);
        List<Outcomes.Proposal> results = new ArrayList<>();
        for (int i = 0; i < 20; i++) cluster.leader().raft().propose(put("k" + i, "v" + i), results::add);
        cluster.runFor(500);
        assertThat(results).hasSize(20).allSatisfy(o -> assertThat(o.status()).isEqualTo(Status.OK));
        long termBefore = cluster.leader().raft().currentTerm();

        for (String id : cluster.nodeIds()) cluster.crash(id);
        cluster.runFor(1_000);
        for (String id : cluster.nodeIds()) cluster.restart(id);
        cluster.runFor(2_000);

        SimNode leader = cluster.leader();
        assertThat(leader).isNotNull();
        assertThat(leader.raft().currentTerm()).as("terms are persisted, so the new term is higher").isGreaterThan(termBefore);
        for (SimNode n : cluster.nodes()) {
            for (int i = 0; i < 20; i++) {
                assertThat(n.kv().get("k" + i)).as(n.id + " k" + i).isNotNull();
                assertThat(n.kv().get("k" + i).value()).isEqualTo("v" + i);
            }
        }
    }

    @Test
    void restartedFollowerCatchesUpFromLeaderLog() {
        SimCluster cluster = started(3, 13);
        String follower = cluster.nodeIds().stream().filter(n -> !n.equals(cluster.leader().id)).findFirst().orElseThrow();
        cluster.crash(follower);
        for (int i = 0; i < 30; i++) cluster.leader().raft().propose(put("k" + i, "v"), r -> { });
        cluster.runFor(500);
        assertThat(cluster.leader().raft().commitIndex()).as("two of three still commit").isGreaterThanOrEqualTo(30);

        cluster.restart(follower);
        cluster.runFor(1_000);
        assertThat(cluster.node(follower).raft().lastApplied()).isEqualTo(cluster.leader().raft().commitIndex());
        assertThat(cluster.node(follower).kv().digest()).isEqualTo(cluster.leader().kv().digest());
    }
}
