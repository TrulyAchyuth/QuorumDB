package io.quorumdb.server.transport;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.RaftMessage;
import io.quorumdb.rpc.Ack;
import io.quorumdb.rpc.Envelope;
import io.quorumdb.rpc.RaftTransportGrpc;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.Tls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Raft transport over gRPC. Each Raft message is one unary {@code Deliver} call, sent asynchronously
 * and never retried: Raft's own heartbeats are the retransmission mechanism. Per-peer in-flight
 * limits bound memory when a peer is slow or down; excess messages are dropped, which Raft tolerates.
 *
 * <p>Inbound envelopes are validated (cluster id, recipient, known sender, well-formed body) before
 * they reach the node; anything else is rejected with INVALID_ARGUMENT / PERMISSION_DENIED.
 */
public final class GrpcTransport implements Transport, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GrpcTransport.class);
    private static final int MAX_MESSAGE_BYTES = 256 << 20;
    private static final int MAX_IN_FLIGHT_PER_PEER = 1024;
    private static final long DEADLINE_MS = 2_000;
    private static final long SNAPSHOT_DEADLINE_MS = 60_000;

    /** Connectivity counters for one peer, exposed to the dashboard. */
    public static final class PeerStats {
        final AtomicLong sent = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicLong dropped = new AtomicLong();
        final AtomicLong received = new AtomicLong();
        final AtomicInteger inFlight = new AtomicInteger();
        volatile long lastSuccessMs = -1;
        volatile long lastReceivedMs = -1;
        volatile String lastError;
    }

    public record PeerView(String peer, String address, long sent, long failed, long dropped, long received,
                           int inFlight, long lastSuccessAgoMs, long lastReceivedAgoMs, String lastError,
                           boolean faultBlocked) {}

    private final ClusterConfig cluster;
    private final Tls tls;
    private final NetworkFaults faults;
    private final Consumer<RaftMessage> inbound;
    private final Server server;
    private final Map<String, RaftTransportGrpc.RaftTransportStub> stubs = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, PeerStats> stats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService delayer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-net-delay");
        t.setDaemon(true);
        return t;
    });

    /** @param tls mutual TLS for the Raft port and outgoing connections, or null for plaintext */
    public GrpcTransport(ClusterConfig cluster, int port, Tls tls, NetworkFaults faults, Consumer<RaftMessage> inbound) {
        this.cluster = cluster;
        this.tls = tls;
        this.faults = faults;
        this.inbound = inbound;
        for (String peer : cluster.peerIds()) stats.put(peer, new PeerStats());
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                .addService(new Service());
        if (tls != null) builder.sslContext(tls.grpcServer());
        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
        log.info("raft transport listening on port {}", server.getPort());
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void send(String to, RaftMessage message) {
        PeerStats s = statsFor(to);
        if (s == null) return;
        if (faults.shouldDrop(to)) {
            s.dropped.incrementAndGet();
            return;
        }
        long delay = faults.delayFor(to);
        if (delay > 0) {
            delayer.schedule(() -> transmit(to, message, s), delay, TimeUnit.MILLISECONDS);
        } else {
            transmit(to, message, s);
        }
    }

    private void transmit(String to, RaftMessage message, PeerStats s) {
        if (s.inFlight.incrementAndGet() > MAX_IN_FLIGHT_PER_PEER) {
            s.inFlight.decrementAndGet();
            s.dropped.incrementAndGet();
            return;
        }
        Envelope envelope = ProtoCodec.encode(cluster.clusterId(), to, message);
        long deadline = message instanceof RaftMessage.InstallSnapshot ? SNAPSHOT_DEADLINE_MS : DEADLINE_MS;
        s.sent.incrementAndGet();
        stub(to).withDeadlineAfter(deadline, TimeUnit.MILLISECONDS).deliver(envelope, new StreamObserver<>() {
            @Override
            public void onNext(Ack value) {
                s.lastSuccessMs = System.currentTimeMillis();
            }

            @Override
            public void onError(Throwable t) {
                s.inFlight.decrementAndGet();
                s.failed.incrementAndGet();
                s.lastError = Status.fromThrowable(t).getCode().name();
            }

            @Override
            public void onCompleted() {
                s.inFlight.decrementAndGet();
                s.lastError = null;
            }
        });
    }

    /** Stats for any node this one knows how to reach (created when a membership change adds one); else null. */
    private PeerStats statsFor(String peer) {
        return cluster.address(peer) != null ? stats.computeIfAbsent(peer, p -> new PeerStats()) : null;
    }

    private RaftTransportGrpc.RaftTransportStub stub(String peer) {
        return stubs.computeIfAbsent(peer, p -> {
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(cluster.address(p).raftAddress())
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .keepAliveTime(30, TimeUnit.SECONDS);
            if (tls == null) {
                builder.usePlaintext();
            } else {
                builder.sslContext(tls.grpcClient());
            }
            ManagedChannel channel = builder.build();
            channels.put(p, channel);
            return RaftTransportGrpc.newStub(channel);
        });
    }

    public Map<String, PeerView> peerViews() {
        long now = System.currentTimeMillis();
        Map<String, PeerView> out = new LinkedHashMap<>();
        for (String peer : cluster.peerIds()) {
            PeerStats s = statsFor(peer);
            if (s == null) continue;
            out.put(peer, new PeerView(peer, cluster.peers().get(peer).raftAddress(), s.sent.get(), s.failed.get(),
                    s.dropped.get(), s.received.get(), s.inFlight.get(),
                    s.lastSuccessMs < 0 ? -1 : now - s.lastSuccessMs,
                    s.lastReceivedMs < 0 ? -1 : now - s.lastReceivedMs, s.lastError, faults.isBlocked(peer)));
        }
        return out;
    }

    private final class Service extends RaftTransportGrpc.RaftTransportImplBase {
        @Override
        public void deliver(Envelope envelope, StreamObserver<Ack> response) {
            if (!cluster.clusterId().equals(envelope.getClusterId())) {
                response.onError(Status.PERMISSION_DENIED.withDescription("wrong cluster id").asRuntimeException());
                return;
            }
            if (!cluster.self().id().equals(envelope.getTo())) {
                response.onError(Status.INVALID_ARGUMENT.withDescription("message addressed to " + envelope.getTo()
                        + " delivered to " + cluster.self().id()).asRuntimeException());
                return;
            }
            PeerStats s = statsFor(envelope.getFrom());
            if (s == null) {
                response.onError(Status.PERMISSION_DENIED.withDescription("unknown sender").asRuntimeException());
                return;
            }
            RaftMessage message;
            try {
                message = ProtoCodec.decode(envelope);
            } catch (RuntimeException e) {
                response.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            }
            // Acknowledge receipt either way: a fault-dropped message looks like a lost packet to the sender.
            response.onNext(Ack.getDefaultInstance());
            response.onCompleted();
            if (faults.shouldDrop(envelope.getFrom())) {
                s.dropped.incrementAndGet();
                return;
            }
            s.received.incrementAndGet();
            s.lastReceivedMs = System.currentTimeMillis();
            inbound.accept(message);
        }
    }

    @Override
    public void close() {
        server.shutdown();
        channels.values().forEach(ManagedChannel::shutdown);
        delayer.shutdownNow();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
            for (ManagedChannel c : channels.values()) c.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        server.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
