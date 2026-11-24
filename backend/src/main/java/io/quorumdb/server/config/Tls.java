package io.quorumdb.server.config;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;

/**
 * TLS for node-to-node traffic, from PEM files ({@code quorumdb.tls-cert}, {@code tls-key}, {@code tls-ca}).
 * The Raft gRPC port uses mutual TLS: each node presents its certificate and only accepts peers whose
 * certificate is signed by the cluster CA. The same CA is trusted by the HTTP clients that forward requests
 * to other members. The REST API's own server certificate is configured with Spring Boot's
 * {@code server.ssl.*} properties.
 */
public final class Tls {
    private final Path cert;
    private final Path key;
    private final Path ca;

    private Tls(Path cert, Path key, Path ca) {
        this.cert = cert;
        this.key = key;
        this.ca = ca;
    }

    /** @return the TLS settings, or null if TLS is off (all three properties empty) */
    public static Tls from(NodeProperties props) {
        boolean any = !props.tlsCert().isBlank() || !props.tlsKey().isBlank() || !props.tlsCa().isBlank();
        if (!any) return null;
        if (props.tlsCert().isBlank() || props.tlsKey().isBlank() || props.tlsCa().isBlank()) {
            throw new IllegalArgumentException("quorumdb.tls-cert, tls-key and tls-ca must be set together");
        }
        return of(Path.of(props.tlsCert()), Path.of(props.tlsKey()), Path.of(props.tlsCa()));
    }

    public static Tls of(Path cert, Path key, Path ca) {
        for (Path p : new Path[] {cert, key, ca}) {
            if (!Files.isReadable(p)) throw new IllegalArgumentException("TLS file " + p + " is not readable");
        }
        return new Tls(cert, key, ca);
    }

    public SslContext grpcServer() {
        try {
            return GrpcSslContexts.forServer(cert.toFile(), key.toFile())
                    .trustManager(ca.toFile()).clientAuth(ClientAuth.REQUIRE).build();
        } catch (IOException e) {
            throw new IllegalStateException("cannot build the gRPC server TLS context", e);
        }
    }

    public SslContext grpcClient() {
        try {
            return GrpcSslContexts.forClient().trustManager(ca.toFile()).keyManager(cert.toFile(), key.toFile()).build();
        } catch (IOException e) {
            throw new IllegalStateException("cannot build the gRPC client TLS context", e);
        }
    }

    /** An HTTP client builder for calls to other members; trusts the cluster CA when TLS is on. */
    public static HttpClient.Builder httpClient(Tls tls) {
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500))
                .version(HttpClient.Version.HTTP_1_1);
        return tls == null ? b : b.sslContext(tls.trustingCa());
    }

    private SSLContext trustingCa() {
        try (InputStream in = Files.newInputStream(ca)) {
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            int i = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                store.setCertificateEntry("ca-" + i++, c);
            }
            if (i == 0) throw new IllegalArgumentException("no certificate in " + ca);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(store);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            return ctx;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("cannot load the CA certificates from " + ca, e);
        }
    }
}
