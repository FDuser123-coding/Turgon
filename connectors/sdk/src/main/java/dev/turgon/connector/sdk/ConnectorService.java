package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import dev.turgon.connector.v1.CheckResponse;
import dev.turgon.connector.v1.ConfigureRequest;
import dev.turgon.connector.v1.ConfigureResponse;
import dev.turgon.connector.v1.ConfirmRequest;
import dev.turgon.connector.v1.ConfirmResponse;
import dev.turgon.connector.v1.ConnectorGrpc;
import dev.turgon.connector.v1.DescribeRequest;
import dev.turgon.connector.v1.DescribeResponse;
import dev.turgon.connector.v1.ExportRecord;
import dev.turgon.connector.v1.ExportRequest;
import dev.turgon.connector.v1.InstanceRequest;
import dev.turgon.connector.v1.PollRequest;
import dev.turgon.connector.v1.PollResponse;
import dev.turgon.connector.v1.Protocol;
import dev.turgon.connector.v1.ReadRequest;
import dev.turgon.connector.v1.ReadResponse;
import dev.turgon.connector.v1.ReleaseResponse;
import dev.turgon.connector.v1.WriteRequest;
import dev.turgon.connector.v1.WriteResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Serves one connector over the connector protocol. Requests name an
 * instance the worker configured; an unknown one (the sidecar restarted)
 * answers ABORTED, and the worker configures it again.
 */
public final class ConnectorService extends ConnectorGrpc.ConnectorImplBase {
    private static final Logger LOG = Logger.getLogger(ConnectorService.class.getName());
    static final ObjectMapper JSON = new ObjectMapper();

    private final Connector connector;
    private final Map<String, Endpoint> instances = new ConcurrentHashMap<>();

    public ConnectorService(Connector connector) {
        this.connector = connector;
    }

    /** The number of configured instances. */
    public int instances() {
        return instances.size();
    }

    @FunctionalInterface
    private interface Call<T> {
        T run() throws Exception;
    }

    private static <T> void answer(StreamObserver<T> out, Call<T> call) {
        T value;
        try {
            value = call.run();
        } catch (Exception e) {
            out.onError(status(e));
            return;
        }
        out.onNext(value);
        out.onCompleted();
    }

    /** Maps an exception to the status the protocol defines for it. */
    static StatusRuntimeException status(Throwable e) {
        if (e instanceof StatusRuntimeException s) {
            return s;
        }
        Status s = switch (e) {
            case ConnectorException.Invalid x -> Status.INVALID_ARGUMENT;
            case ConnectorException.Rejected x -> Status.FAILED_PRECONDITION;
            case ConnectorException.NotFound x -> Status.NOT_FOUND;
            case ConnectorException.Unsupported x -> Status.UNIMPLEMENTED;
            default -> {
                LOG.log(Level.WARNING, "connector call failed", e);
                yield Status.UNAVAILABLE;
            }
        };
        String msg = e instanceof ConnectorException ? e.getMessage() : e.getClass().getSimpleName() + ": " + e.getMessage();
        return s.withDescription(msg).asRuntimeException();
    }

    private Endpoint instance(String id) {
        Endpoint e = instances.get(id);
        if (e == null) {
            throw Status.ABORTED.withDescription("instance " + id + " is not configured").asRuntimeException();
        }
        return e;
    }

    private static JsonNode json(ByteString b, String what) throws ConnectorException.Invalid {
        if (b.isEmpty()) {
            return JSON.nullNode();
        }
        try {
            return JSON.readTree(b.toByteArray());
        } catch (IOException e) {
            throw new ConnectorException.Invalid(what + " is not JSON");
        }
    }

    private static ByteString bytes(JsonNode n) throws IOException {
        return n == null ? ByteString.EMPTY : ByteString.copyFrom(JSON.writeValueAsBytes(n));
    }

    @Override
    public void describe(DescribeRequest req, StreamObserver<DescribeResponse> out) {
        answer(out, () -> {
            var b = DescribeResponse.newBuilder().setProtocol(Protocol.PROTOCOL_V1)
                    .setConnector(connector.name()).setVersion(connector.version());
            connector.capabilities().forEach(c -> b.addCapabilities(c.wire()));
            return b.build();
        });
    }

    @Override
    public void configure(ConfigureRequest req, StreamObserver<ConfigureResponse> out) {
        answer(out, () -> {
            if (req.getInstance().isEmpty()) {
                throw new ConnectorException.Invalid("the instance has no ID");
            }
            JsonNode spec = json(req.getSpec(), "the spec");
            if (!connector.name().equals(spec.path("name").asText())) {
                throw new ConnectorException.Invalid("the spec is for connector " + spec.path("name").asText() + ", this is " + connector.name());
            }
            var cfg = new EndpointConfig(req.getEndpoint(), spec.path("name").asText(), spec.path("version").asText(),
                    spec.get("config"), req.getSecret(), req.getSecretsMap());
            Endpoint e = connector.bind(cfg);
            Endpoint old = instances.put(req.getInstance(), e);
            if (old != null) {
                close(old);
            }
            LOG.info(() -> "configured " + cfg.endpoint() + " as " + req.getInstance());
            return ConfigureResponse.getDefaultInstance();
        });
    }

    private static void close(Endpoint e) {
        try {
            e.close();
        } catch (Exception x) {
            LOG.log(Level.WARNING, "closing an endpoint failed", x);
        }
    }

    @Override
    public void release(InstanceRequest req, StreamObserver<ReleaseResponse> out) {
        answer(out, () -> {
            Endpoint e = instances.remove(req.getInstance());
            if (e != null) {
                close(e);
            }
            return ReleaseResponse.getDefaultInstance();
        });
    }

    @Override
    public void check(InstanceRequest req, StreamObserver<CheckResponse> out) {
        answer(out, () -> {
            var b = CheckResponse.newBuilder();
            for (CheckResult r : instance(req.getInstance()).check()) {
                b.addResults(dev.turgon.connector.v1.CheckResult.newBuilder().setName(r.name()).setOk(r.ok())
                        .setDetail(r.detail() == null ? "" : r.detail()).setFix(r.fix() == null ? "" : r.fix()));
            }
            return b.build();
        });
    }

    @Override
    public void simulate(WriteRequest req, StreamObserver<WriteResponse> out) {
        answer(out, () -> WriteResponse.newBuilder()
                .setResult(bytes(instance(req.getInstance()).simulate(req.getOperation(), json(req.getPayload(), "the payload"))))
                .build());
    }

    @Override
    public void commit(WriteRequest req, StreamObserver<WriteResponse> out) {
        answer(out, () -> {
            if (req.getIdempotencyKey().isEmpty()) {
                throw new ConnectorException.Invalid("the write has no idempotency key");
            }
            return WriteResponse.newBuilder().setResult(bytes(instance(req.getInstance())
                    .commit(req.getOperation(), req.getIdempotencyKey(), json(req.getPayload(), "the payload")))).build();
        });
    }

    @Override
    public void confirm(ConfirmRequest req, StreamObserver<ConfirmResponse> out) {
        answer(out, () -> {
            instance(req.getInstance()).confirm(req.getOperation(), json(req.getResult(), "the result"));
            return ConfirmResponse.getDefaultInstance();
        });
    }

    @Override
    public void read(ReadRequest req, StreamObserver<ReadResponse> out) {
        answer(out, () -> ReadResponse.newBuilder()
                .setRecord(bytes(instance(req.getInstance()).read(req.getOperation(), req.getId()))).build());
    }

    @Override
    public void poll(PollRequest req, StreamObserver<PollResponse> out) {
        answer(out, () -> {
            var b = PollResponse.newBuilder();
            for (Event e : instance(req.getInstance()).poll(req.getEvent(), req.getAfter(), req.getLimit())) {
                b.addEvents(dev.turgon.connector.v1.Event.newBuilder().setId(e.id()).setPosition(e.position())
                        .setName(e.name()).setPayload(bytes(e.payload())));
            }
            return b.build();
        });
    }

    @Override
    public void export(ExportRequest req, StreamObserver<ExportRecord> out) {
        var call = (ServerCallStreamObserver<ExportRecord>) out;
        try {
            instance(req.getInstance()).export(req.getName(), rec -> {
                if (call.isCancelled()) {
                    throw Status.CANCELLED.withDescription("the worker stopped reading").asRuntimeException();
                }
                out.onNext(ExportRecord.newBuilder().putAllFields(rec).build());
            });
        } catch (Exception e) {
            out.onError(status(e));
            return;
        }
        out.onCompleted();
    }
}
