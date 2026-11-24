package io.quorumdb.server;

import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.NodeProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterConfigTest {

    private static NodeProperties props(String id, String peers, String token) {
        return new NodeProperties(id, "c1", "/tmp/x", 9101, peers, "http://localhost:8081/", 150, 300, 50, 1000, true, true,
                true, 5000, 256, 1024, token, 100, "", "", "");
    }

    @Test
    void parsesPeers() {
        ClusterConfig c = ClusterConfig.from(props("n1",
                "n2=host2:9102=http://host2:8082, n3=host3:9103=http://host3:8083/", ""));
        assertThat(c.size()).isEqualTo(3);
        assertThat(c.quorum()).isEqualTo(2);
        assertThat(c.peerIds()).containsExactly("n2", "n3");
        assertThat(c.member("n3").httpBaseUrl()).isEqualTo("http://host3:8083");
        assertThat(c.self().httpBaseUrl()).isEqualTo("http://localhost:8081");
        assertThat(c.members()).extracting(ClusterConfig.Member::id).containsExactly("n1", "n2", "n3");
    }

    @Test
    void singleNodeClusterHasNoPeers() {
        ClusterConfig c = ClusterConfig.from(props("n1", "", ""));
        assertThat(c.size()).isEqualTo(1);
        assertThat(c.quorum()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> ClusterConfig.from(props("n1", "n1=h:1=http://h:2", ""))).hasMessageContaining("must not include");
        assertThatThrownBy(() -> ClusterConfig.from(props("n1", "n2=h:1=http://h:2,n2=h:3=http://h:4", ""))).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> ClusterConfig.from(props("n1", "n2=h=http://h:2", ""))).hasMessageContaining("raft address");
        assertThatThrownBy(() -> ClusterConfig.from(props("n1", "n2:h:1", ""))).hasMessageContaining("must look like");
        assertThatThrownBy(() -> ClusterConfig.from(props("bad id!", "", ""))).hasMessageContaining("node-id");
        assertThatThrownBy(() -> ClusterConfig.from(props("n1", "", "short"))).hasMessageContaining("16 characters");
    }
}
