package io.quorumdb.server.transport;

import com.google.protobuf.ByteString;
import io.quorumdb.raft.LogEntry;
import io.quorumdb.raft.RaftMessage;
import io.quorumdb.rpc.AppendEntries;
import io.quorumdb.rpc.AppendEntriesReply;
import io.quorumdb.rpc.Entry;
import io.quorumdb.rpc.Envelope;
import io.quorumdb.rpc.InstallSnapshot;
import io.quorumdb.rpc.RequestVote;
import io.quorumdb.rpc.RequestVoteReply;

import java.util.ArrayList;
import java.util.List;

/** Converts between core {@link RaftMessage}s and their protobuf wire form, validating on the way in. */
public final class ProtoCodec {
    private ProtoCodec() {}

    public static Envelope encode(String clusterId, String to, RaftMessage message) {
        Envelope.Builder env = Envelope.newBuilder().setClusterId(clusterId).setFrom(message.from()).setTo(to)
                .setTerm(message.term());
        switch (message) {
            case RaftMessage.RequestVote m -> env.setRequestVote(RequestVote.newBuilder()
                    .setLastLogIndex(m.lastLogIndex()).setLastLogTerm(m.lastLogTerm()).setPreVote(m.preVote()));
            case RaftMessage.RequestVoteReply m -> env.setRequestVoteReply(RequestVoteReply.newBuilder()
                    .setRequestTerm(m.requestTerm()).setGranted(m.granted()).setPreVote(m.preVote()));
            case RaftMessage.AppendEntries m -> {
                AppendEntries.Builder b = AppendEntries.newBuilder().setPrevLogIndex(m.prevLogIndex())
                        .setPrevLogTerm(m.prevLogTerm()).setLeaderCommit(m.leaderCommit()).setReadSeq(m.readSeq());
                for (LogEntry e : m.entries()) {
                    b.addEntries(Entry.newBuilder().setIndex(e.index()).setTerm(e.term()).setType(e.type().ordinal())
                            .setData(ByteString.copyFrom(e.data())));
                }
                env.setAppendEntries(b);
            }
            case RaftMessage.AppendEntriesReply m -> env.setAppendEntriesReply(AppendEntriesReply.newBuilder()
                    .setSuccess(m.success()).setMatchIndex(m.matchIndex()).setConflictIndex(m.conflictIndex())
                    .setConflictTerm(m.conflictTerm()).setReadSeq(m.readSeq()));
            case RaftMessage.InstallSnapshot m -> env.setInstallSnapshot(InstallSnapshot.newBuilder()
                    .setLastIncludedIndex(m.lastIncludedIndex()).setLastIncludedTerm(m.lastIncludedTerm())
                    .setData(ByteString.copyFrom(m.data())).setReadSeq(m.readSeq()));
        }
        return env.build();
    }

    /**
     * @throws IllegalArgumentException if the envelope is malformed (missing body, impossible values)
     */
    public static RaftMessage decode(Envelope env) {
        String from = env.getFrom();
        long term = env.getTerm();
        if (term < 0) throw new IllegalArgumentException("negative term");
        return switch (env.getBodyCase()) {
            case REQUEST_VOTE -> {
                RequestVote m = env.getRequestVote();
                yield new RaftMessage.RequestVote(from, term, nonNegative(m.getLastLogIndex()),
                        nonNegative(m.getLastLogTerm()), m.getPreVote());
            }
            case REQUEST_VOTE_REPLY -> {
                RequestVoteReply m = env.getRequestVoteReply();
                yield new RaftMessage.RequestVoteReply(from, term, nonNegative(m.getRequestTerm()), m.getGranted(), m.getPreVote());
            }
            case APPEND_ENTRIES -> {
                AppendEntries m = env.getAppendEntries();
                List<LogEntry> entries = new ArrayList<>(m.getEntriesCount());
                long expected = nonNegative(m.getPrevLogIndex()) + 1;
                for (Entry e : m.getEntriesList()) {
                    if (e.getIndex() != expected++) throw new IllegalArgumentException("entries are not contiguous");
                    if (e.getTerm() > term) throw new IllegalArgumentException("entry term exceeds leader term");
                    entries.add(new LogEntry(e.getIndex(), e.getTerm(), LogEntry.Type.fromOrdinal(e.getType()),
                            e.getData().toByteArray()));
                }
                yield new RaftMessage.AppendEntries(from, term, m.getPrevLogIndex(), nonNegative(m.getPrevLogTerm()),
                        entries, nonNegative(m.getLeaderCommit()), nonNegative(m.getReadSeq()));
            }
            case APPEND_ENTRIES_REPLY -> {
                AppendEntriesReply m = env.getAppendEntriesReply();
                yield new RaftMessage.AppendEntriesReply(from, term, m.getSuccess(), nonNegative(m.getMatchIndex()),
                        nonNegative(m.getConflictIndex()), nonNegative(m.getConflictTerm()), nonNegative(m.getReadSeq()));
            }
            case INSTALL_SNAPSHOT -> {
                InstallSnapshot m = env.getInstallSnapshot();
                if (m.getLastIncludedIndex() < 1 || m.getLastIncludedTerm() < 1) {
                    throw new IllegalArgumentException("snapshot must cover at least one entry");
                }
                yield new RaftMessage.InstallSnapshot(from, term, m.getLastIncludedIndex(), m.getLastIncludedTerm(),
                        m.getData().toByteArray(), nonNegative(m.getReadSeq()));
            }
            case BODY_NOT_SET -> throw new IllegalArgumentException("envelope has no body");
        };
    }

    /** protobuf uint64 arrives as a Java long; values >= 2^63 show up negative and are rejected. */
    private static long nonNegative(long v) {
        if (v < 0) throw new IllegalArgumentException("value out of range");
        return v;
    }
}
