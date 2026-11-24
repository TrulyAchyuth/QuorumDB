package io.quorumdb.server;

import io.quorumdb.server.config.Tls;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * A QuorumDB cluster of real OS processes started from the built server jar, for integration tests.
 * {@link #kill} is SIGKILL: no shutdown hooks run, exactly like a crash. {@link #close} force-kills
 * every process it started, so a failing test never leaves processes behind.
 */
final class ProcessCluster implements AutoCloseable {
    static final String TOKEN = "integration-test-token-0123456789";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    record Response(int status, JsonNode body, String raw) {}

    private final Path dir;
    private final int size;
    private final int[] httpPorts;
    private final int[] grpcPorts;
    private final Map<String, Process> processes = new LinkedHashMap<>();
    private final Path tls; // directory with ca.pem, node.pem, node.key; null = plaintext
    private final String scheme;
    private final HttpClient http;

    ProcessCluster(Path dir, int size, Path tls) throws IOException {
        this.dir = dir;
        this.size = size;
        this.tls = tls;
        this.scheme = tls == null ? "http" : "https";
        this.http = Tls.httpClient(tls == null ? null
                : Tls.of(tls.resolve("node.pem"), tls.resolve("node.key"), tls.resolve("ca.pem"))).build();
        this.httpPorts = new int[size + 1];
        this.grpcPorts = new int[size + 1];
        for (int i = 1; i <= size; i++) {
            httpPorts[i] = freePort();
            grpcPorts[i] = freePort();
        }
    }

    static ProcessCluster start(Path dir, int size) throws Exception {
        return start(dir, size, null);
    }

    /** Starts the cluster with TLS on the Raft and HTTP ports if {@code tls} (see {@link #generateTls}) is given. */
    static ProcessCluster start(Path dir, int size, Path tls) throws Exception {
        ProcessCluster c = new ProcessCluster(dir, size, tls);
        for (int i = 1; i <= size; i++) c.startNode("n" + i);
        for (int i = 1; i <= size; i++) c.awaitHttp("n" + i);
        return c;
    }

    void startNode(String id) throws IOException {
        int i = Integer.parseInt(id.substring(1));
        StringBuilder peers = new StringBuilder();
        for (int j = 1; j <= size; j++) {
            if (j == i) continue;
            if (!peers.isEmpty()) peers.append(',');
            peers.append("n").append(j).append("=localhost:").append(grpcPorts[j]).append("=").append(scheme)
                    .append("://localhost:").append(httpPorts[j]);
        }
        String jar = System.getProperty("quorumdb.serverJar");
        if (jar == null || !Files.exists(Path.of(jar))) throw new IllegalStateException("server jar not built: " + jar);
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> cmd = new ArrayList<>(List.of(java, "-Xmx256m", "-XX:TieredStopAtLevel=1", "-jar", jar,
                "--server.port=" + httpPorts[i], "--quorumdb.node-id=" + id, "--quorumdb.grpc-port=" + grpcPorts[i],
                "--quorumdb.data-dir=" + dir.resolve("data").resolve(id), "--quorumdb.peers=" + peers,
                "--quorumdb.advertised-http=" + scheme + "://localhost:" + httpPorts[i], "--quorumdb.admin-token=" + TOKEN,
                "--quorumdb.request-timeout-ms=3000"));
        if (tls != null) {
            String cert = tls.resolve("node.pem").toString();
            String key = tls.resolve("node.key").toString();
            cmd.addAll(List.of("--quorumdb.tls-cert=" + cert, "--quorumdb.tls-key=" + key,
                    "--quorumdb.tls-ca=" + tls.resolve("ca.pem"), "--server.ssl.enabled=true",
                    "--server.ssl.certificate=" + cert, "--server.ssl.certificate-private-key=" + key));
        }
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve(id + ".log").toFile())).start();
        processes.put(id, p);
    }

    void awaitHttp(String id) {
        await(Duration.ofSeconds(60), () -> get(id, "/api/v1/health").status() > 0, id + " did not start: " + logTail(id));
    }

    /** SIGKILL: the process dies instantly without running any shutdown logic. */
    void kill(String id) throws InterruptedException {
        Process p = processes.remove(id);
        if (p == null) return;
        p.destroyForcibly();
        p.waitFor(10, TimeUnit.SECONDS);
    }

    void restart(String id) throws IOException {
        startNode(id);
        awaitHttp(id);
    }

    List<String> ids() {
        List<String> out = new ArrayList<>();
        for (int i = 1; i <= size; i++) out.add("n" + i);
        return out;
    }

    List<String> running() {
        return List.copyOf(processes.keySet());
    }

    /** Leader as seen by {@code via}'s cluster view, or null. */
    String leader(String via) {
        Response r = get(via, "/api/v1/leader");
        return r.status() == 200 ? r.body().get("leaderId").asString() : null;
    }

    String awaitLeader(String via, Duration timeout) {
        String[] found = new String[1];
        await(timeout, () -> (found[0] = leader(via)) != null, "no leader elected");
        return found[0];
    }

    Response get(String id, String path) {
        return call(id, HttpRequest.newBuilder(uri(id, path)).GET());
    }

    Response put(String id, String key, String value) {
        return call(id, HttpRequest.newBuilder(uri(id, "/api/v1/kv/" + key)).header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of("value", value)))));
    }

    /** POST with a JSON body and no credentials. */
    Response post(String id, String path, Object body) {
        return call(id, HttpRequest.newBuilder(uri(id, path)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))));
    }

    Response admin(String id, String method, String path, Object body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri(id, path)).header("Authorization", "Bearer " + TOKEN)
                .header("Content-Type", "application/json");
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        return call(id, b);
    }

    private Response call(String id, HttpRequest.Builder b) {
        try {
            HttpResponse<String> r = http.send(b.timeout(Duration.ofSeconds(10)).build(), HttpResponse.BodyHandlers.ofString());
            JsonNode body = null;
            try {
                body = r.body().isEmpty() ? null : JSON.readTree(r.body());
            } catch (RuntimeException ignored) {
                // not JSON
            }
            return new Response(r.statusCode(), body, r.body());
        } catch (IOException e) {
            return new Response(-1, null, e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Response(-1, null, "interrupted");
        }
    }

    private URI uri(String id, String path) {
        return URI.create(scheme + "://localhost:" + httpPorts[Integer.parseInt(id.substring(1))] + path);
    }

    /** Plain-HTTP URI for a node, to check that a TLS node does not serve it. */
    URI plaintextUri(String id, String path) {
        return URI.create("http://localhost:" + httpPorts[Integer.parseInt(id.substring(1))] + path);
    }

    /**
     * Creates a throwaway CA and one certificate for localhost (server and client use) with openssl.
     * Skips the calling test if openssl is not installed.
     */
    static Path generateTls(Path dir) throws IOException, InterruptedException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("ext.cnf"), "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
                + "extendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n");
        List<List<String>> steps = List.of(
                List.of("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem",
                        "-days", "2", "-subj", "/CN=quorumdb-test-ca", "-addext", "basicConstraints=critical,CA:TRUE",
                        "-addext", "keyUsage=critical,keyCertSign"),
                List.of("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", "node.key", "-out", "node.csr",
                        "-subj", "/CN=localhost"),
                List.of("openssl", "x509", "-req", "-in", "node.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
                        "-CAcreateserial", "-out", "node.pem", "-days", "2", "-extfile", "ext.cnf"));
        for (List<String> step : steps) {
            Process p;
            try {
                p = new ProcessBuilder(step).directory(dir.toFile()).redirectErrorStream(true).start();
            } catch (IOException e) {
                org.junit.jupiter.api.Assumptions.abort("openssl is not available: " + e.getMessage());
                throw e;
            }
            String out = new String(p.getInputStream().readAllBytes());
            if (p.waitFor() != 0) throw new IllegalStateException(String.join(" ", step) + " failed:\n" + out);
        }
        return dir;
    }

    String logTail(String id) {
        try {
            List<String> lines = Files.readAllLines(dir.resolve(id + ".log"));
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 30), lines.size()));
        } catch (IOException e) {
            return "(no log)";
        }
    }

    static void await(Duration timeout, BooleanSupplier condition, String message) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted");
            }
        }
        throw new AssertionError(message + " (after " + timeout.toMillis() + " ms)");
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    @Override
    public void close() {
        for (Process p : processes.values()) p.destroyForcibly();
        for (Process p : processes.values()) {
            try {
                p.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        processes.clear();
    }
}
