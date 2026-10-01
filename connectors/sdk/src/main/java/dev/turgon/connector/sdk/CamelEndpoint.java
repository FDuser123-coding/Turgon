package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.errorhandler.DefaultErrorHandlerDefinition;

/**
 * An endpoint whose operations are Camel routes: {@code direct:<operation>}
 * receives the payload as a {@link JsonNode} body and the headers below,
 * and its result is the exchange's body. Routes use Camel's components and
 * enterprise integration patterns (splitting, enrichment, retries towards
 * the system); the worker keeps policy, approval, idempotency and audit.
 */
public abstract class CamelEndpoint implements Endpoint {
    /** simulate, commit, confirm or read. */
    public static final String MODE = "TurgonMode";
    public static final String OPERATION = "TurgonOperation";
    public static final String IDEMPOTENCY_KEY = "TurgonIdempotencyKey";
    /** The record ID of a read. */
    public static final String ID = "TurgonId";

    private final CamelContext camel;
    private final ProducerTemplate producer;

    protected CamelEndpoint(String name, RoutesBuilder routes) throws Exception {
        camel = new DefaultCamelContext();
        camel.getCamelContextExtension().setName("turgon-" + name);
        // Failures go back to the worker, which decides and audits; the
        // sidecar logs only unexpected ones (ConnectorService), never bodies.
        camel.getCamelContextExtension().setErrorHandlerFactory(new DefaultErrorHandlerDefinition().logExhausted(false));
        camel.addRoutes(routes);
        camel.start();
        producer = camel.createProducerTemplate();
    }

    public CamelContext camel() {
        return camel;
    }

    /** Sends one exchange to the operation's route and returns its body. */
    protected JsonNode send(String mode, String operation, JsonNode body, String key, String id) throws Exception {
        if (camel.getRoute(operation) == null) {
            throw new ConnectorException.Invalid("the connector has no operation " + operation);
        }
        Exchange ex = producer.send("direct:" + operation, e -> {
            e.getIn().setHeader(MODE, mode);
            e.getIn().setHeader(OPERATION, operation);
            if (key != null) {
                e.getIn().setHeader(IDEMPOTENCY_KEY, key);
            }
            if (id != null) {
                e.getIn().setHeader(ID, id);
            }
            e.getIn().setBody(body);
        });
        Exception failed = ex.getException();
        if (failed instanceof CamelExecutionException c && c.getCause() instanceof Exception cause) {
            failed = cause;
        }
        if (failed != null) {
            throw failed;
        }
        return ex.getMessage().getBody(JsonNode.class);
    }

    @Override
    public JsonNode simulate(String operation, JsonNode payload) throws Exception {
        return send("simulate", operation, payload, null, null);
    }

    @Override
    public JsonNode commit(String operation, String key, JsonNode payload) throws Exception {
        return send("commit", operation, payload, key, null);
    }

    @Override
    public void confirm(String operation, JsonNode result) throws Exception {
        send("confirm", operation, result, null, null);
    }

    @Override
    public JsonNode read(String operation, String id) throws Exception {
        return send("read", operation, null, null, id);
    }

    @Override
    public void close() throws Exception {
        producer.close();
        camel.close();
    }
}
