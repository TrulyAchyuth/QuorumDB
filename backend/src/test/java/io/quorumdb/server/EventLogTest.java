package io.quorumdb.server;

import io.quorumdb.raft.RaftEvent;
import io.quorumdb.server.node.EventLog;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventLogTest {
    private static RaftEvent event() {
        return new RaftEvent(0, 1, "n1", RaftEvent.Type.HEARTBEAT_SENT, 1, 0, 0, 0, null, null);
    }

    @Test
    void assignsGapFreeSequenceNumbersAndPagesForward() {
        EventLog log = new EventLog(10);
        for (int i = 0; i < 5; i++) log.append(event());
        EventLog.Page page = log.after(2, 100);
        assertThat(page.events()).extracting(RaftEvent::seq).containsExactly(3L, 4L, 5L);
        assertThat(page.truncated()).isFalse();
        assertThat(log.after(0, 2).events()).extracting(RaftEvent::seq).containsExactly(1L, 2L);
        assertThat(log.after(5, 10).events()).isEmpty();
    }

    @Test
    void latestOfGivenTypesIsNotCrowdedOutByFrequentEvents() {
        EventLog log = new EventLog(100);
        log.append(new RaftEvent(0, 1, "n1", RaftEvent.Type.LEADER_ELECTED, 1, 0, 0, 0, null, null));
        for (int i = 0; i < 50; i++) log.append(event());
        assertThat(log.latest(10)).noneMatch(e -> e.type() == RaftEvent.Type.LEADER_ELECTED);
        assertThat(log.latest(10, java.util.Set.of(RaftEvent.Type.LEADER_ELECTED))).singleElement()
                .satisfies(e -> assertThat(e.seq()).isEqualTo(1));
    }

    @Test
    void rareEventsOutliveTheMainRingUnderWriteLoad() {
        EventLog log = new EventLog(100);
        log.append(new RaftEvent(0, 1, "n1", RaftEvent.Type.LEADER_ELECTED, 1, 0, 0, 0, null, null));
        for (int i = 0; i < 1_000; i++) {
            log.append(new RaftEvent(0, 2, "n1", RaftEvent.Type.ENTRY_COMMITTED, 1, i, 1, 0, null, null));
        }
        assertThat(log.after(0, 10).truncated()).as("main ring has overwritten it").isTrue();
        assertThat(log.latest(5, java.util.Set.of(RaftEvent.Type.LEADER_ELECTED, RaftEvent.Type.STEPPED_DOWN)))
                .extracting(RaftEvent::seq).containsExactly(1L);
    }

    @Test
    void reportsWhenRequestedEventsWereOverwritten() {
        EventLog log = new EventLog(10);
        for (int i = 0; i < 25; i++) log.append(event());
        EventLog.Page page = log.after(3, 100);
        assertThat(page.truncated()).as("events 4..15 are gone").isTrue();
        assertThat(page.oldestSeq()).isEqualTo(16);
        assertThat(page.events()).extracting(RaftEvent::seq).startsWith(16L).endsWith(25L);
        assertThat(log.latest(3)).extracting(RaftEvent::seq).containsExactly(23L, 24L, 25L);
    }
}
