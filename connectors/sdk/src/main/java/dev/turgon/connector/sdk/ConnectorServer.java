package dev.turgon.connector.sdk;

import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollIoHandler;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.logging.Logger;

/**
 * The sidecar: serves the connector found on the class path over the
 * connector protocol.
 *
 * <pre>
 * TURGON_CONNECTOR_LISTEN  unix:///var/run/turgon/connectors/sap-ecc.sock (default), or host:port
 * TURGON_CONNECTOR_TOKEN   if set, the worker must send it as a bearer token (required for TCP)
 * TURGON_CONNECTOR         which connector to serve, when the class path carries several
 * </pre>
 */
public final class ConnectorServer {
    private static final Logger LOG = Logger.getLogger(ConnectorServer.class.getName());
    static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private ConnectorServer() {}

    /** Finds the connector to serve: the only one, or the one named. */
    public static Connector find(String name) {
        List<Connector> all = new ArrayList<>();
        ServiceLoader.load(Connector.class).forEach(all::add);
        if (name != null && !name.isEmpty()) {
            return all.stream().filter(c -> c.name().equals(name)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("no connector " + name + " on the class path"));
        }
        if (all.size() != 1) {
            throw new IllegalStateException(all.isEmpty() ? "no connector on the class path"
                    : "several connectors on the class path: set TURGON_CONNECTOR");
        }
        return all.getFirst();
    }

    /** Requires the bearer token on every call, compared in constant time. */
    public static ServerInterceptor token(String token) {
        byte[] want = ("Bearer " + token).getBytes(StandardCharsets.UTF_8);
        return new ServerInterceptor() {
            @Override
            public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                String got = headers.get(AUTHORIZATION);
                if (got == null || !MessageDigest.isEqual(want, got.getBytes(StandardCharsets.UTF_8))) {
                    call.close(Status.UNAUTHENTICATED.withDescription("a bearer token is required"), new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return next.startCall(call, headers);
            }
        };
    }

    /** Builds the server for listen ("unix:///path" or "host:port"); not started. */
    public static Server build(Connector connector, String listen, String token) throws IOException {
        NettyServerBuilder b;
        if (listen.startsWith("unix://")) {
            Path sock = Path.of(listen.substring("unix://".length()));
            if (sock.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 107) {
                throw new IllegalArgumentException("the socket path " + sock + " is longer than Unix sockets allow (107 bytes)");
            }
            Files.createDirectories(sock.getParent());
            Files.deleteIfExists(sock); // left by a previous run
            b = NettyServerBuilder.forAddress(new DomainSocketAddress(sock.toString()))
                    .channelType(EpollServerDomainSocketChannel.class)
                    .bossEventLoopGroup(new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory()))
                    .workerEventLoopGroup(new MultiThreadIoEventLoopGroup(EpollIoHandler.newFactory()));
        } else {
            int i = listen.lastIndexOf(':');
            if (i < 0) {
                throw new IllegalArgumentException("TURGON_CONNECTOR_LISTEN " + listen + ": want unix:///path or host:port");
            }
            String host = listen.substring(0, i).replaceAll("^\\[|]$", "");
            // Whoever reaches the sidecar can write to its system without the
            // worker's governance: over TCP, even loopback, only with the token.
            if (token == null || token.isEmpty()) {
                throw new IllegalArgumentException("listening on " + listen + " needs TURGON_CONNECTOR_TOKEN (or use unix:///path)");
            }
            b = NettyServerBuilder.forAddress(new InetSocketAddress(host, Integer.parseInt(listen.substring(i + 1))));
        }
        var service = new ConnectorService(connector);
        b.maxInboundMessageSize(16 << 20);
        if (token != null && !token.isEmpty()) {
            b.addService(ServerInterceptors.intercept(service, token(token)));
        } else {
            b.addService(service);
        }
        return b.build();
    }

    public static void main(String[] args) throws Exception {
        String listen = env("TURGON_CONNECTOR_LISTEN", "");
        Connector connector = find(System.getenv("TURGON_CONNECTOR"));
        if (listen.isEmpty()) {
            listen = "unix:///var/run/turgon/connectors/" + connector.name() + ".sock";
        }
        String token = env("TURGON_CONNECTOR_TOKEN", "");
        Server server = build(connector, listen, token).start();
        if (listen.startsWith("unix://")) {
            // Only the pod's containers mount the socket's volume; still, not world-writable.
            Files.setPosixFilePermissions(Path.of(listen.substring(7)), PosixFilePermissions.fromString("rw-rw----"));
        }
        LOG.info(() -> "turgon connector " + connector.name() + " " + connector.version() + " listening on "
                + System.getenv().getOrDefault("TURGON_CONNECTOR_LISTEN", "the default socket")
                + (token.isEmpty() ? "" : " (token required)"));
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.awaitTermination();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }
}
