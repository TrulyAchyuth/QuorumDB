package io.quorumdb.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Node configuration, bound from {@code quorumdb.*} (application.yml, environment variables such as
 * {@code QUORUMDB_NODE_ID}, or command-line arguments).
 *
 * @param peers         other members as {@code id=host:grpcPort=httpBaseUrl}, comma separated, e.g.
 *                      {@code n2=localhost:9102=http://localhost:8082,n3=localhost:9103=http://localhost:8083}
 * @param fsync         fsync every WAL write (true) or leave flushing to the OS (benchmarks only)
 * @param adminToken    bearer token for fault injection and internal endpoints. Empty disables them.
 * @param maxKeyBytes   maximum UTF-8 size of a key
 * @param maxValueBytes maximum UTF-8 size of a value
 * @param tlsCert       PEM certificate chain this node presents on the Raft port and to peers; empty = no TLS
 * @param tlsKey        PEM (PKCS#8) private key for {@code tlsCert}
 * @param tlsCa         PEM CA certificate(s) that peers' certificates must be signed by
 */
@ConfigurationProperties("quorumdb")
public record NodeProperties(
        @DefaultValue("n1") String nodeId,
        @DefaultValue("quorumdb") String clusterId,
        @DefaultValue("./data/n1") String dataDir,
        @DefaultValue("9101") int grpcPort,
        @DefaultValue("") String peers,
        @DefaultValue("http://localhost:8080") String advertisedHttp,
        @DefaultValue("150") long electionTimeoutMinMs,
        @DefaultValue("300") long electionTimeoutMaxMs,
        @DefaultValue("50") long heartbeatIntervalMs,
        @DefaultValue("10000") long snapshotThreshold,
        @DefaultValue("true") boolean preVote,
        @DefaultValue("true") boolean checkQuorum,
        @DefaultValue("true") boolean fsync,
        @DefaultValue("5000") long requestTimeoutMs,
        @DefaultValue("256") int maxKeyBytes,
        @DefaultValue("1048576") int maxValueBytes,
        @DefaultValue("") String adminToken,
        @DefaultValue("20000") int eventBufferSize,
        @DefaultValue("") String tlsCert,
        @DefaultValue("") String tlsKey,
        @DefaultValue("") String tlsCa) {
}
