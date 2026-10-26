package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * One endpoint bound to its system. Only {@link #commit} is required; the
 * others throw {@link ConnectorException.Unsupported} unless the connector
 * declares the matching {@link Capability}.
 *
 * <p>Throw {@link ConnectorException.Invalid} for a payload the system cannot
 * take, {@link ConnectorException.Rejected} when the system refuses the write,
 * {@link ConnectorException.NotFound} for a missing record. Any other
 * exception is transient: the worker retries. Messages reach operators: name
 * fields, never their values.
 */
public interface Endpoint extends AutoCloseable {

    /** Dry-runs a write and returns a preview of the resulting document. */
    default JsonNode simulate(String operation, JsonNode payload) throws Exception {
        throw new ConnectorException.Unsupported("simulate");
    }

    /**
     * Performs a write. A retried write carries the same idempotency key:
     * look it up in the system (a reference field) before writing again.
     */
    JsonNode commit(String operation, String idempotencyKey, JsonNode payload) throws Exception;

    /** Reads a committed write back; throws when it is not there. */
    default void confirm(String operation, JsonNode result) throws Exception {
        throw new ConnectorException.Unsupported("confirm");
    }

    default JsonNode read(String operation, String id) throws Exception {
        throw new ConnectorException.Unsupported("read");
    }

    /** Up to {@code limit} events named {@code event} with position &gt; {@code after}, in order. */
    default List<Event> poll(String event, long after, int limit) throws Exception {
        throw new ConnectorException.Unsupported("poll");
    }

    /** The events the system pushes, delivered by {@link #stream} rather than polled. */
    default Set<String> streams() {
        return Set.of();
    }

    /**
     * Subscribes to a pushed event until the returned subscription is closed.
     * Hand what arrives to {@link StreamSink#deliver}, which returns once the
     * worker has stored it: confirm it to the system only then, and fail the
     * system's call when deliver throws, so the system sends it again.
     */
    default AutoCloseable stream(String event, byte[] resume, StreamSink sink) throws Exception {
        throw new ConnectorException.Unsupported("stream");
    }

    /**
     * The events that may also arrive by webhook: the worker takes each
     * delivery on its webhook listener and hands it to {@link #receive}.
     * They are still polled, which reconciles what the system never sent.
     */
    default Set<String> webhooks() {
        return Set.of();
    }

    /**
     * Why an event cannot arrive by webhook although the endpoint is
     * configured for it (its signing secret is not set, say), by event. The
     * event is still polled; a worker listening for webhooks refuses to
     * start with the reason.
     */
    default Map<String, String> webhookErrors() {
        return Map.of();
    }

    /**
     * Verifies one webhook delivery (its signature, with the secret for it)
     * and returns the events it carries; their positions are ignored. Use
     * the same event IDs as {@link #poll}, so the worker runs each event
     * once. Throw {@link ConnectorException.Unauthenticated} for a missing,
     * wrong or expired signature; return no events for an authentic
     * delivery of another event.
     *
     * @param headers the delivery's HTTP headers, case-insensitive
     */
    default List<Event> receive(String event, Map<String, List<String>> headers, byte[] body, java.time.Instant received) throws Exception {
        throw new ConnectorException.Unsupported("webhook");
    }

    default void export(String name, Consumer<Map<String, String>> each) throws Exception {
        throw new ConnectorException.Unsupported("export");
    }

    /**
     * What the system holds, for the metadata graph: the objects the
     * configuration uses and those named, as
     * {@code {"objects": [{"name", "kind", "fields": [...], "links": [...]}], "uses": [...]}}
     * (Turgon's pkg/meta.Catalog).
     */
    default JsonNode discover(List<String> objects) throws Exception {
        throw new ConnectorException.Unsupported("discover");
    }

    default List<CheckResult> check() throws Exception {
        throw new ConnectorException.Unsupported("check");
    }

    @Override
    default void close() throws Exception {}
}
