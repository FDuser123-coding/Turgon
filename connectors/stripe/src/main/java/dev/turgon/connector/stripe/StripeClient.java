package dev.turgon.connector.stripe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turgon.connector.sdk.ConnectorException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Calls the Stripe API: form-encoded requests with the key as a Bearer
 * token, an Idempotency-Key on every write, and Stripe's errors turned
 * into what the worker acts on. Stripe's own retry advice
 * ({@code Stripe-Should-Retry}) decides when it is not plain.
 */
final class StripeClient {
    /** Stripe refused the key. */
    static final class Unauthorized extends IOException {
        private static final long serialVersionUID = 1L;

        Unauthorized(String message) {
            super(message);
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final StripeConfig cfg;
    private final String key;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    StripeClient(StripeConfig cfg, String key) {
        this.cfg = cfg;
        this.key = key == null ? "" : key.trim();
    }

    /** "test" or "live", from the key's prefix; "" when it is not a secret or restricted key. */
    String mode() {
        if (key.startsWith("sk_test_") || key.startsWith("rk_test_")) {
            return "test";
        }
        if (key.startsWith("sk_live_") || key.startsWith("rk_live_")) {
            return "live";
        }
        return "";
    }

    static String form(Map<String, String> params) {
        StringJoiner j = new StringJoiner("&");
        params.forEach((k, v) -> j.add(URLEncoder.encode(k, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(v, StandardCharsets.UTF_8)));
        return j.toString();
    }

    JsonNode get(String path, Map<String, String> query) throws Exception {
        return send("GET", path + (query.isEmpty() ? "" : "?" + form(query)), null, null);
    }

    JsonNode post(String path, Map<String, String> params, String idempotencyKey) throws Exception {
        return send("POST", path, form(params), idempotencyKey);
    }

    JsonNode delete(String path) throws Exception {
        return send("DELETE", path, null, null);
    }

    private JsonNode send(String method, String path, String body, String idempotencyKey) throws Exception {
        if (key.isEmpty()) {
            throw new Unauthorized("the secret holds no Stripe key");
        }
        if (key.startsWith("pk_")) {
            throw new ConnectorException.Invalid("the secret is a publishable key (pk_...), which cannot call these APIs: use a restricted key (rk_...)");
        }
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(cfg.url() + path)).timeout(Duration.ofSeconds(60))
                .header("Authorization", "Bearer " + key).header("Accept", "application/json");
        if (!cfg.apiVersion().isEmpty()) {
            b.header("Stripe-Version", cfg.apiVersion());
        }
        if (idempotencyKey != null) {
            b.header("Idempotency-Key", idempotencyKey.length() > 255 ? idempotencyKey.substring(0, 255) : idempotencyKey);
        }
        if (body != null) {
            b.header("Content-Type", "application/x-www-form-urlencoded").method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        int s = resp.statusCode();
        JsonNode out = parse(resp.body());
        if (s / 100 == 2) {
            return out;
        }
        JsonNode err = out.path("error");
        String type = err.path("type").asText(""), code = err.path("code").asText(""), param = err.path("param").asText("");
        String msg = "Stripe answered " + s + (code.isEmpty() ? "" : " " + code) + ": " + err.path("message").asText("") + (param.isEmpty() ? "" : " (" + param + ")");
        String retry = resp.headers().firstValue("Stripe-Should-Retry").orElse("");
        if (retry.equals("true") || s == 429 || s / 100 == 5 || s == 409 && !type.equals("idempotency_error")) {
            throw new IOException(msg + "; the worker retries");
        }
        switch (s) {
            case 401 -> throw new Unauthorized(msg);
            case 402 -> throw new ConnectorException.Rejected(msg);
            case 403 -> throw new ConnectorException.Rejected(msg + " Grant the restricted key that permission in the Stripe Dashboard.");
            case 404 -> throw new ConnectorException.NotFound(msg);
            default -> throw new ConnectorException.Invalid(msg);
        }
    }

    private static JsonNode parse(String s) {
        try {
            JsonNode n = s == null || s.isBlank() ? null : JSON.readTree(s);
            return n == null ? JSON.createObjectNode() : n;
        } catch (IOException x) {
            return JSON.createObjectNode();
        }
    }
}
