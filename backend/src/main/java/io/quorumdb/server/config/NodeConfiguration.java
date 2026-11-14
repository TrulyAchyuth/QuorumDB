package io.quorumdb.server.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.quorumdb.raft.RaftStatus;
import io.quorumdb.server.metrics.RequestMetrics;
import io.quorumdb.server.node.NodeRuntime;
import io.quorumdb.server.node.OperationLog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.function.ToDoubleFunction;

/** Wires one node: configuration, the Raft runtime and metrics. */
@Configuration
public class NodeConfiguration {

    @Bean
    ClusterConfig clusterConfig(NodeProperties props) {
        return ClusterConfig.from(props);
    }

    @Bean(destroyMethod = "close")
    NodeRuntime nodeRuntime(NodeProperties props, ClusterConfig cluster) throws IOException {
        NodeRuntime runtime = new NodeRuntime(props, cluster);
        runtime.start();
        return runtime;
    }

    @Bean
    PrometheusMeterRegistry prometheusMeterRegistry(NodeRuntime runtime, ClusterConfig cluster) {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("node", cluster.self().id(), "cluster", cluster.clusterId());
        gauge(registry, runtime, "quorumdb.raft.term", "Current Raft term", s -> s.term());
        gauge(registry, runtime, "quorumdb.raft.commit.index", "Commit index", s -> s.commitIndex());
        gauge(registry, runtime, "quorumdb.raft.applied.index", "Last applied index", s -> s.lastApplied());
        gauge(registry, runtime, "quorumdb.raft.log.last.index", "Last log index", s -> s.lastLogIndex());
        gauge(registry, runtime, "quorumdb.raft.snapshot.index", "Snapshot index", s -> s.snapshotIndex());
        gauge(registry, runtime, "quorumdb.raft.is.leader", "1 if this node is the leader", s ->
                s.running() && s.role() == io.quorumdb.raft.RaftNode.Role.LEADER ? 1 : 0);
        gauge(registry, runtime, "quorumdb.raft.pending.proposals", "Uncommitted proposals held by the leader",
                s -> s.pendingProposals());
        return registry;
    }

    private static void gauge(PrometheusMeterRegistry registry, NodeRuntime runtime, String name, String help,
                              ToDoubleFunction<RaftStatus> f) {
        Gauge.builder(name, runtime, r -> {
            RaftStatus s = r.status();
            return s == null ? Double.NaN : f.applyAsDouble(s);
        }).description(help).register(registry);
    }

    @Bean
    io.quorumdb.server.faultlab.SimulationLimits simulationLimits() {
        io.quorumdb.server.faultlab.SimulationLimits limits = io.quorumdb.server.faultlab.SimulationLimits.forThisJvm();
        org.slf4j.LoggerFactory.getLogger(NodeConfiguration.class).info(
                "FaultLab simulation limits: {} events per run, {} at once, {} events stored", limits.maxEventsPerRun(),
                limits.maxConcurrent(), limits.maxStoredEvents());
        return limits;
    }

    @Bean
    RequestMetrics requestMetrics(PrometheusMeterRegistry registry) {
        return new RequestMetrics(registry);
    }

    @Bean
    OperationLog operationLog() {
        return new OperationLog(2_000);
    }
}
