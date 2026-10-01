package dev.turgon.connector.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import dev.turgon.connector.v1.ConfigureRequest;
import dev.turgon.connector.v1.ConfirmRequest;
import dev.turgon.connector.v1.ConnectorGrpc;
import dev.turgon.connector.v1.DescribeRequest;
import dev.turgon.connector.v1.ExportRecord;
import dev.turgon.connector.v1.ExportRequest;
import dev.turgon.connector.v1.InstanceRequest;
import dev.turgon.connector.v1.PollRequest;
import dev.turgon.connector.v1.Protocol;
import dev.turgon.connector.v1.ReadRequest;
import dev.turgon.connector.v1.WriteRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollIoHandler;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.grpc.stub.MetadataUtils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConnectorServiceTest {

    /** An order system with one customer, its operations as Camel routes. */
    static final class Orders extends CamelEndpoint {
        static final Map<String, String> ORDERS = new ConcurrentHashMap<>();

        Orders() throws Exception {
            super("orders", new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:create-order").routeId("create-order").process(ex -> {
                        JsonNode p = ex.getIn().getBody(JsonNode.class);
                        String customer = p.path("customerId").asText();
                        if (customer.isEmpty()) {
                            throw new ConnectorException.Invalid("customerId is required");
                        }
                        if (!customer.equals("C-100")) {
                            throw new ConnectorException.Rejected("the customer is blocked for sales");
                        }
                        ObjectNode out = ConnectorService.JSON.createObjectNode();
                        switch (ex.getIn().getHeader(MODE, String.class)) {
                            case "simulate" -> out.put("testRun", true);
                            case "confirm" -> {
                                if (!ORDERS.containsValue(p.path("order").asText())) {
                                    throw new ConnectorException.NotFound("the order is not there");
                                }
                            }
                            default -> out.put("order", ORDERS.computeIfAbsent(
                                    ex.getIn().getHeader(IDEMPOTENCY_KEY, String.class), k -> "SO-" + (ORDERS.size() + 1)));
                        }
                        ex.getMessage().setBody(out);
                    });
                    from("direct:get-customer").routeId("get-customer").process(ex -> {
                        String id = ex.getIn().getHeader(ID, String.class);
                        if (!id.equals("C-100")) {
                            throw new ConnectorException.NotFound("no such customer");
                        }
                        ex.getMessage().setBody(ConnectorService.JSON.readTree("{\"id\":\"C-100\"}"));
                    });
                    from("direct:boom").routeId("boom").process(ex -> {
                        throw new IllegalStateException("the system timed out");
                    });
                }
            });
        }

        @Override
        public void confirm(String operation, JsonNode result) throws Exception {
            send("confirm", operation, ConnectorService.JSON.createObjectNode().put("customerId", "C-100").set("order", result.path("order")), null, null);
        }

        @Override
        public List<Event> poll(String event, long after, int limit) {
            List<Event> out = new ArrayList<>();
            for (long p = after + 1; p <= 3 && out.size() < limit; p++) {
                out.add(new Event("e" + p, p, event, ConnectorService.JSON.createObjectNode()));
            }
            return out;
        }

        @Override
        public void export(String name, Consumer<Map<String, String>> each) {
            each.accept(Map.of("id", "C-100"));
            each.accept(Map.of("id", "C-200"));
        }

        @Override
        public List<CheckResult> check() {
            return List.of(CheckResult.pass("system", "answers"));
        }
    }

    static final class OrdersConnector implements Connector {
        final List<EndpointConfig> bound = new ArrayList<>();

        @Override
        public String name() {
            return "orders";
        }

        @Override
        public String version() {
            return "1.2.0";
        }

        @Override
        public Set<Capability> capabilities() {
            return EnumSet.allOf(Capability.class);
        }

        @Override
        public Endpoint bind(EndpointConfig config) throws Exception {
            if (!config.secret().equals("user:pw")) {
                throw new ConnectorException.Invalid("the secret is not user:password");
            }
            bound.add(config);
            return new Orders();
        }
    }

    @TempDir
    Path dir;
    Server server;
    ManagedChannel channel;
    MultiThreadIoEventLoopGroup group;
    OrdersConnector connector;
    ConnectorGrpc.ConnectorBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        connector = new OrdersConnector();
        Path sock = dir.resolve("orders.sock");
        server = ConnectorServer.build(connector, "unix://" + sock, "tok").start();
        group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
        channel = NettyChannelBuilder.forAddress(new DomainSocketAddress(sock.toString()))
                .channelType(EpollDomainSocketChannel.class).eventLoopGroup(group).usePlaintext().build();
        Metadata md = new Metadata();
        md.put(ConnectorServer.AUTHORIZATION, "Bearer tok");
        stub = ConnectorGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
        Orders.ORDERS.clear();
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        group.shutdownGracefully();
    }

    static ByteString json(String s) {
        return ByteString.copyFrom(s, StandardCharsets.UTF_8);
    }

    void configure(String instance) {
        stub.configure(ConfigureRequest.newBuilder().setInstance(instance).setEndpoint("erp")
                .setSpec(json("{\"endpoint\":\"erp\",\"name\":\"orders\",\"version\":\"1.2.0\",\"config\":{\"client\":\"100\"}}"))
                .setSecret("user:pw").putSecrets("openbao://erp/hook", "whsec").build());
    }

    static Status.Code code(Runnable r) {
        return assertThrows(StatusRuntimeException.class, r::run).getStatus().getCode();
    }

    @Test
    void servesTheProtocol() {
        var d = stub.describe(DescribeRequest.newBuilder().setProtocol(Protocol.PROTOCOL_V1).build());
        assertEquals("orders", d.getConnector());
        assertEquals("1.2.0", d.getVersion());
        assertEquals(List.of("simulate", "confirm", "read", "poll", "export", "check"), d.getCapabilitiesList());

        configure("erp-1");
        EndpointConfig cfg = connector.bound.getFirst();
        assertEquals("erp", cfg.endpoint());
        assertEquals("100", cfg.config().path("client").asText());
        assertEquals("whsec", cfg.secrets().get("openbao://erp/hook"));
        assertTrue(!cfg.toString().contains("user:pw"));

        var w = WriteRequest.newBuilder().setInstance("erp-1").setOperation("create-order")
                .setIdempotencyKey("K1").setPayload(json("{\"customerId\":\"C-100\"}")).build();
        assertEquals("{\"testRun\":true}", stub.simulate(w).getResult().toStringUtf8());
        String result = stub.commit(w).getResult().toStringUtf8();
        assertEquals("{\"order\":\"SO-1\"}", result);
        assertEquals(result, stub.commit(w).getResult().toStringUtf8()); // the same key: the same order
        stub.confirm(ConfirmRequest.newBuilder().setInstance("erp-1").setOperation("create-order").setResult(json(result)).build());
        assertEquals(Status.Code.NOT_FOUND, code(() -> stub.confirm(ConfirmRequest.newBuilder().setInstance("erp-1")
                .setOperation("create-order").setResult(json("{\"order\":\"SO-9\"}")).build())));

        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.commit(w.toBuilder().setPayload(json("{}")).build())));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.commit(w.toBuilder().setPayload(json("{")).build())));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.commit(w.toBuilder().setIdempotencyKey("").build())));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.commit(w.toBuilder().setOperation("nope").build())));
        assertEquals(Status.Code.FAILED_PRECONDITION, code(() -> stub.commit(w.toBuilder().setPayload(json("{\"customerId\":\"C-9\"}")).build())));
        var boom = assertThrows(StatusRuntimeException.class, () -> stub.commit(w.toBuilder().setOperation("boom").build()));
        assertEquals(Status.Code.UNAVAILABLE, boom.getStatus().getCode());
        assertEquals("IllegalStateException: the system timed out", boom.getStatus().getDescription());

        assertEquals("{\"id\":\"C-100\"}", stub.read(ReadRequest.newBuilder().setInstance("erp-1")
                .setOperation("get-customer").setId("C-100").build()).getRecord().toStringUtf8());
        assertEquals(Status.Code.NOT_FOUND, code(() -> stub.read(ReadRequest.newBuilder().setInstance("erp-1")
                .setOperation("get-customer").setId("C-9").build())));

        var events = stub.poll(PollRequest.newBuilder().setInstance("erp-1").setEvent("Order.Created").setAfter(1).setLimit(10).build());
        assertEquals(2, events.getEventsCount());
        assertEquals(3, events.getEvents(1).getPosition());

        Iterator<ExportRecord> it = stub.export(ExportRequest.newBuilder().setInstance("erp-1").setName("customers").build());
        List<String> ids = new ArrayList<>();
        it.forEachRemaining(r -> ids.add(r.getFieldsMap().get("id")));
        assertEquals(List.of("C-100", "C-200"), ids);

        assertTrue(stub.check(InstanceRequest.newBuilder().setInstance("erp-1").build()).getResults(0).getOk());
    }

    @Test
    void unknownInstancesAreAborted() {
        var w = WriteRequest.newBuilder().setInstance("erp-1").setOperation("create-order").setIdempotencyKey("K1")
                .setPayload(json("{\"customerId\":\"C-100\"}")).build();
        assertEquals(Status.Code.ABORTED, code(() -> stub.commit(w)));
        configure("erp-1");
        stub.commit(w);
        stub.release(InstanceRequest.newBuilder().setInstance("erp-1").build());
        assertEquals(Status.Code.ABORTED, code(() -> stub.commit(w)));
    }

    @Test
    void refusesBadConfigurations() {
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.configure(ConfigureRequest.newBuilder().setInstance("x")
                .setSpec(json("{\"name\":\"other\"}")).setSecret("user:pw").build())));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.configure(ConfigureRequest.newBuilder().setInstance("x")
                .setSpec(json("{\"name\":\"orders\"}")).setSecret("nope").build())));
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> stub.configure(ConfigureRequest.newBuilder()
                .setSpec(json("{\"name\":\"orders\"}")).setSecret("user:pw").build())));
    }

    @Test
    void requiresTheToken() {
        var bare = ConnectorGrpc.newBlockingStub(channel);
        assertEquals(Status.Code.UNAUTHENTICATED, code(() -> bare.describe(DescribeRequest.getDefaultInstance())));
        Metadata md = new Metadata();
        md.put(ConnectorServer.AUTHORIZATION, "Bearer wrong");
        var wrong = bare.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
        assertEquals(Status.Code.UNAUTHENTICATED, code(() -> wrong.describe(DescribeRequest.getDefaultInstance())));
    }

    @Test
    void tcpNeedsAToken() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ConnectorServer.build(connector, "0.0.0.0:0", ""));
        assertThrows(IllegalArgumentException.class, () -> ConnectorServer.build(connector, "127.0.0.1:0", ""));
        ConnectorServer.build(connector, "127.0.0.1:0", "tok").start().shutdownNow();
        assertThrows(IllegalArgumentException.class, () -> ConnectorServer.build(connector, "nonsense", ""));
        assertThrows(IllegalArgumentException.class, () -> ConnectorServer.build(connector, "unix:///tmp/" + "x".repeat(120) + ".sock", ""));
    }
}
