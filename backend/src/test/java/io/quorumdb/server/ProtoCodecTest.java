package io.quorumdb.server;

import io.quorumdb.raft.LogEntry;
import io.quorumdb.raft.RaftMessage;
import io.quorumdb.rpc.AppendEntries;
import io.quorumdb.rpc.Entry;
import io.quorumdb.rpc.Envelope;
import io.quorumdb.server.transport.ProtoCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtoCodecTest {

    private static RaftMessage roundTrip(RaftMessage m) throws Exception {
        Envelope env = ProtoCodec.encode("c", "n2", m);
        assertThat(env.getClusterId()).isEqualTo("c");
        assertThat(env.getTo()).isEqualTo("n2");
        return ProtoCodec.decode(Envelope.parseFrom(env.toByteArray()));
    }

    @Test
    void everyMessageTypeRoundTrips() throws Exception {
        List<RaftMessage> messages = List.of(
                new RaftMessage.RequestVote("n1", 5, 10, 4, true),
                new RaftMessage.RequestVoteReply("n1", 5, 6, true, false),
                new RaftMessage.AppendEntriesReply("n1", 5, false, 0, 7, 3, 9),
                new RaftMessage.InstallSnapshot("n1", 5, 100, 4, new byte[] {1, 2, 3}, 2));
        for (RaftMessage m : messages) {
            RaftMessage back = roundTrip(m);
            if (m instanceof RaftMessage.InstallSnapshot s) {
                RaftMessage.InstallSnapshot b = (RaftMessage.InstallSnapshot) back;
                assertThat(b.data()).containsExactly(s.data());
                assertThat(b.lastIncludedIndex()).isEqualTo(100);
            } else {
                assertThat(back).isEqualTo(m);
            }
        }
        RaftMessage.AppendEntries append = new RaftMessage.AppendEntries("n1", 3, 4, 2,
                List.of(new LogEntry(5, 3, LogEntry.Type.NOOP, new byte[0]), new LogEntry(6, 3, LogEntry.Type.COMMAND, "x".getBytes())),
                4, 11);
        RaftMessage.AppendEntries back = (RaftMessage.AppendEntries) roundTrip(append);
        assertThat(back.entries()).hasSize(2);
        assertThat(back.entries().get(1).checksum()).isEqualTo(append.entries().get(1).checksum());
        assertThat(back.readSeq()).isEqualTo(11);
    }

    @Test
    void malformedEnvelopesAreRejected() {
        assertThatThrownBy(() -> ProtoCodec.decode(Envelope.newBuilder().setFrom("n1").setTerm(1).build()))
                .hasMessageContaining("no body");
        Envelope gap = Envelope.newBuilder().setFrom("n1").setTerm(2).setAppendEntries(AppendEntries.newBuilder()
                .setPrevLogIndex(1).addEntries(Entry.newBuilder().setIndex(3).setTerm(1))).build();
        assertThatThrownBy(() -> ProtoCodec.decode(gap)).hasMessageContaining("contiguous");
        Envelope futureTerm = Envelope.newBuilder().setFrom("n1").setTerm(2).setAppendEntries(AppendEntries.newBuilder()
                .setPrevLogIndex(0).addEntries(Entry.newBuilder().setIndex(1).setTerm(9))).build();
        assertThatThrownBy(() -> ProtoCodec.decode(futureTerm)).hasMessageContaining("exceeds");
        Envelope badType = Envelope.newBuilder().setFrom("n1").setTerm(2).setAppendEntries(AppendEntries.newBuilder()
                .addEntries(Entry.newBuilder().setIndex(1).setTerm(1).setType(7))).build();
        assertThatThrownBy(() -> ProtoCodec.decode(badType)).isInstanceOf(IllegalArgumentException.class);
        Envelope overflow = Envelope.newBuilder().setFrom("n1").setTerm(1).setAppendEntriesReply(
                io.quorumdb.rpc.AppendEntriesReply.newBuilder().setMatchIndex(-1)).build();
        assertThatThrownBy(() -> ProtoCodec.decode(overflow)).hasMessageContaining("out of range");
    }
}
