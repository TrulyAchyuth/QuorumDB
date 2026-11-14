package io.quorumdb.faultlab;

import io.quorumdb.faultlab.check.NodeObservation;
import io.quorumdb.faultlab.check.SafetyChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.sim.SimCluster;
import io.quorumdb.faultlab.sim.SimOptions;
import io.quorumdb.kv.KvCommand;
import io.quorumdb.raft.LogEntry;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.storage.RaftStorage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Does the safety checker have teeth? These tests run the real Raft code on top of deliberately
 * broken storage and require the checker to notice. A checker that only ever says "SAFE" proves
 * nothing; one that flags seeded bugs and passes the unmodified system is evidence.
 */
class SeededBugTest {

    /** Storage that "forgets" which candidate it voted for: the node will vote for anyone who asks. */
    private static final class ForgetfulVoteStorage extends Delegating {
        ForgetfulVoteStorage(RaftStorage d) {
            super(d);
        }

        @Override
        public String votedFor() {
            return null;
        }
    }

    /** Storage that silently refuses to remove conflicting entries (a broken truncate). */
    private static final class NoTruncateStorage extends Delegating {
        NoTruncateStorage(RaftStorage d) {
            super(d);
        }

        @Override
        public void truncateSuffix(long fromIndex) {
            // bug: keeps the stale suffix
        }

        @Override
        public void append(List<LogEntry> entries) {
            // With the stale suffix still there, appends no longer line up; overwrite in place.
            for (LogEntry e : entries) {
                if (e.index() <= delegate.lastIndex()) continue; // bug: stale entry survives at this index
                delegate.append(List.of(e));
            }
        }
    }

    private static class Delegating implements RaftStorage {
        final RaftStorage delegate;

        Delegating(RaftStorage delegate) {
            this.delegate = delegate;
        }

        @Override public long currentTerm() { return delegate.currentTerm(); }
        @Override public String votedFor() { return delegate.votedFor(); }
        @Override public void saveHardState(long term, String votedFor) { delegate.saveHardState(term, votedFor); }
        @Override public long snapshotIndex() { return delegate.snapshotIndex(); }
        @Override public long snapshotTerm() { return delegate.snapshotTerm(); }
        @Override public byte[] loadSnapshot() { return delegate.loadSnapshot(); }
        @Override public long lastIndex() { return delegate.lastIndex(); }
        @Override public long termAt(long index) { return delegate.termAt(index); }
        @Override public LogEntry entry(long index) { return delegate.entry(index); }
        @Override public List<LogEntry> entries(long from, long to) { return delegate.entries(from, to); }
        @Override public void append(List<LogEntry> entries) { delegate.append(entries); }
        @Override public void truncateSuffix(long fromIndex) { delegate.truncateSuffix(fromIndex); }
        @Override public void saveSnapshot(long index, long term, byte[] data) { delegate.saveSnapshot(index, term, data); }
        @Override public void installSnapshot(long index, long term, byte[] data) { delegate.installSnapshot(index, term, data); }
        @Override public void close() { delegate.close(); }
    }

    private record Audit(SafetyReport report, long commits, long truncations, long elections) {}

    /**
     * Runs a cluster under repeated partitions with a simple write load and returns the audit.
     * Tight election timeouts and no PreVote/CheckQuorum make concurrent candidacies and divergent
     * logs common: an isolated leader keeps accepting writes that must later be truncated.
     */
    private static Audit audit(long seed, UnaryOperator<RaftStorage> storageBug) {
        SimOptions options = new SimOptions(3, seed, 150, 160, 50, false, false, 0, 1, 30, false, 50);
        SimCluster cluster = new SimCluster(options, storageBug);
        SafetyChecker checker = new SafetyChecker(false);
        cluster.addListener(checker::onEvent);
        cluster.startAll();
        int command = 0;
        for (int round = 0; round < 60; round++) {
            // Propose through whoever currently believes it is leader (possibly several nodes).
            for (SimCluster.SimNode n : cluster.nodes()) {
                if (n.isServing() && n.raft().isLeader()) {
                    byte[] put = KvCommand.single("", 0, KvCommand.Op.put("k" + command % 5, "v" + command)).encode();
                    command++;
                    n.raft().propose(put, r -> { });
                }
            }
            if (round % 4 == 1) {
                // Alternate between isolating the current leader and a fixed node.
                SimCluster.SimNode leader = cluster.leader();
                cluster.network().isolate(leader != null && round % 8 == 1 ? leader.id : "n" + (1 + round / 4 % 3), true);
            }
            if (round % 4 == 3) cluster.network().healAll();
            cluster.runFor(120);
            List<NodeObservation> observations = new ArrayList<>();
            for (String id : cluster.nodeIds()) observations.add(cluster.observe(id));
            checker.onObservations(observations);
        }
        long commits = cluster.events().stream().filter(e -> e.type() == RaftEvent.Type.ENTRY_COMMITTED).count();
        long truncations = cluster.events().stream().filter(e -> e.type() == RaftEvent.Type.LOG_TRUNCATED).count();
        long elections = cluster.events().stream().filter(e -> e.type() == RaftEvent.Type.LEADER_ELECTED).count();
        return new Audit(checker.report(), commits, truncations, elections);
    }

    @Test
    void unmodifiedRaftPassesTheSameHarness() {
        long truncations = 0, commits = 0;
        for (long seed = 1; seed <= 30; seed++) {
            Audit audit = audit(seed, UnaryOperator.identity());
            assertThat(audit.report().verdict()).as("seed " + seed + ": " + audit.report().checks())
                    .isEqualTo(SafetyReport.Verdict.SAFE);
            // Guard against a vacuous pass: the harness must really elect leaders and commit entries.
            // (The 10 ms timeout spread against up to 30 ms latency causes split votes on purpose, so
            // some seeds spend a while leaderless; hence a per-seed floor plus an aggregate one.)
            assertThat(audit.commits()).as("seed " + seed + " commits").isGreaterThan(10);
            commits += audit.commits();
            assertThat(audit.elections()).as("seed " + seed + " elections").isGreaterThanOrEqualTo(2);
            truncations += audit.truncations();
        }
        assertThat(commits).as("commit events across all seeds").isGreaterThan(2_000);
        assertThat(truncations).as("the harness must produce divergent logs that need truncation").isGreaterThan(10);
    }

    @Test
    void forgettingVotesIsCaughtAsTwoLeadersInOneTerm() {
        int caught = 0;
        for (long seed = 1; seed <= 30; seed++) {
            SafetyReport report = audit(seed, ForgetfulVoteStorage::new).report();
            boolean electionSafetyViolated = report.checks().stream()
                    .anyMatch(c -> c.id().equals("ELECTION_SAFETY") && c.status() == SafetyReport.Status.VIOLATED);
            if (electionSafetyViolated) caught++;
        }
        assertThat(caught).as("double voting must surface as an Election Safety violation in some seeds").isPositive();
    }

    @Test
    void brokenTruncationIsCaughtAsLogOrCommitViolation() {
        int caught = 0;
        for (long seed = 1; seed <= 30; seed++) {
            if (audit(seed, NoTruncateStorage::new).report().hasViolations()) caught++;
        }
        assertThat(caught).as("stale entries surviving conflict resolution must be detected in most seeds")
                .isGreaterThanOrEqualTo(15);
    }
}
