package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
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

    default void export(String name, Consumer<Map<String, String>> each) throws Exception {
        throw new ConnectorException.Unsupported("export");
    }

    default List<CheckResult> check() throws Exception {
        throw new ConnectorException.Unsupported("check");
    }

    @Override
    default void close() throws Exception {}
}
