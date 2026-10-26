package dev.turgon.connector.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.ConnectorException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Sends named GraphQL operations to the Admin API. Shopify answers most
 * failures with HTTP 200 and an {@code errors} array: a throttled query
 * (the cost bucket is empty) is retried by the worker; a missing access
 * scope is not, since retrying cannot grant it.
 */
final class ShopifyClient {
    /** The API refused the token. */
    static final class Unauthorized extends IOException {
        private static final long serialVersionUID = 1L;

        Unauthorized(String message) {
            super(message);
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final ShopifyConfig cfg;
    private final String token;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    /** The version Shopify served the last response with. */
    private volatile String served = "";

    ShopifyClient(ShopifyConfig cfg, String token) {
        this.cfg = cfg;
        this.token = token == null ? "" : token.trim();
    }

    String served() {
        return served;
    }

    /** Runs an operation and returns its data. */
    JsonNode run(String operation, String query, ObjectNode variables) throws Exception {
        if (token.isEmpty()) {
            throw new Unauthorized("the secret holds no access token");
        }
        ObjectNode body = JSON.createObjectNode().put("query", query).put("operationName", operation);
        body.set("variables", variables == null ? JSON.createObjectNode() : variables);
        HttpRequest req = HttpRequest.newBuilder(cfg.endpoint()).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json").header("Accept", "application/json")
                .header("X-Shopify-Access-Token", token).POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body))).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        served = resp.headers().firstValue("X-Shopify-API-Version").orElse("");
        int s = resp.statusCode();
        JsonNode out = parse(resp.body());
        if (s == 401 || s == 403) {
            throw new Unauthorized("Shopify answered " + s + ": " + text(out.path("errors")));
        }
        if (s == 404) {
            throw new ConnectorException.Invalid("Shopify answered 404 for " + cfg.endpoint().getPath() + ": check shop and apiVersion");
        }
        if (s == 402 || s == 423) {
            throw new ConnectorException.Rejected("Shopify answered " + s + ": the store is frozen or locked (unpaid plan or fraud review)");
        }
        if (s == 429 || s / 100 == 5) {
            throw new IOException("Shopify answered " + s + resp.headers().firstValue("Retry-After").map(r -> "; retry after " + r + " s").orElse(""));
        }
        if (s != 200) {
            throw new ConnectorException.Invalid("Shopify answered " + s + ": " + text(out.path("errors")));
        }
        JsonNode errs = out.path("errors");
        if (errs.isArray() && !errs.isEmpty()) {
            JsonNode first = errs.get(0);
            String code = first.path("extensions").path("code").asText("");
            String msg = first.path("message").asText("");
            switch (code) {
                case "THROTTLED" -> throw new IOException("Shopify throttled the query (the API's cost limit); the worker retries");
                case "ACCESS_DENIED" -> throw new ConnectorException.Rejected("Shopify denied access: " + msg + " Add the scope to the app and reinstall it.");
                case "INTERNAL_SERVER_ERROR" -> throw new IOException("Shopify failed: " + msg);
                default -> throw new ConnectorException.Invalid("Shopify refused the " + operation + " query: " + msg);
            }
        }
        return out.path("data");
    }

    private static JsonNode parse(String s) {
        try {
            JsonNode n = s == null || s.isBlank() ? null : JSON.readTree(s);
            return n == null ? JSON.createObjectNode() : n;
        } catch (IOException x) {
            return JSON.createObjectNode();
        }
    }

    private static String text(JsonNode errors) {
        String t = errors.isTextual() ? errors.asText() : errors.isMissingNode() ? "" : errors.toString();
        return t.length() > 300 ? t.substring(0, 300) + "…" : t;
    }
}
