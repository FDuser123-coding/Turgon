package dev.turgon.connector.powerbi;

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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Calls the Power BI REST API as a service principal: a token from
 * Microsoft Entra (client credentials, the Power BI API's .default scope),
 * kept until shortly before it expires and fetched again when the API
 * rejects it.
 */
final class PowerBiClient {
    static final String SCOPE = "https://analysis.windows.net/powerbi/api/.default";

    /** The API answered with an error. */
    static final class ApiError extends IOException {
        private static final long serialVersionUID = 1L;
        final int status;
        final String code;
        final String retryAfter;

        ApiError(int status, String code, String message, String retryAfter) {
            super(message);
            this.status = status;
            this.code = code;
            this.retryAfter = retryAfter;
        }
    }

    /** Entra refused the credentials. */
    static final class SignInError extends IOException {
        private static final long serialVersionUID = 1L;
        final String code;
        final String aadsts;

        SignInError(String code, String description) {
            super(code + ": " + firstSentence(description));
            this.code = code;
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("AADSTS(\\d+)").matcher(description);
            this.aadsts = m.find() ? m.group(1) : "";
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private final PowerBiConfig cfg;
    private final String secret;
    private final HttpClient http;
    private final Clock clock;
    private String token;
    private Instant expires = Instant.EPOCH;

    PowerBiClient(PowerBiConfig cfg, String secret, Clock clock) {
        this.cfg = cfg;
        this.secret = secret == null ? "" : secret;
        this.clock = clock;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    /** A valid access token, signing in when there is none. */
    synchronized String token() throws IOException, InterruptedException {
        if (token != null && clock.instant().isBefore(expires)) {
            return token;
        }
        if (secret.isBlank()) {
            throw new SignInError("invalid_client", "AADSTS7000216: The secret is empty.");
        }
        String form = "grant_type=client_credentials&client_id=" + url(cfg.clientId()) + "&client_secret=" + url(secret) + "&scope=" + url(SCOPE);
        HttpRequest req = HttpRequest.newBuilder(URI.create(cfg.authority() + "/" + PowerBiConfig.encode(cfg.tenant()) + "/oauth2/v2.0/token"))
                .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        JsonNode body = parse(resp.body());
        if (resp.statusCode() != 200) {
            if (body.has("error")) {
                throw new SignInError(body.path("error").asText(), body.path("error_description").asText(""));
            }
            throw new IOException("Microsoft Entra answered " + resp.statusCode());
        }
        token = body.path("access_token").asText("");
        if (token.isEmpty()) {
            throw new IOException("Microsoft Entra's answer has no access_token");
        }
        // Renew a minute early, so a token never expires in flight.
        expires = clock.instant().plusSeconds(Math.max(0, body.path("expires_in").asLong(3599) - 60));
        return token;
    }

    private synchronized void forget(String used) {
        if (used.equals(token)) {
            token = null;
        }
    }

    /** Calls the API below the dataset's path; null body: no content. Signs in again once when the token is rejected. */
    JsonNode call(String method, String path, JsonNode body) throws IOException, InterruptedException {
        for (int attempt = 0;; attempt++) {
            String t = token();
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(cfg.api() + cfg.datasetPath() + path)).timeout(Duration.ofSeconds(60))
                    .header("Authorization", "Bearer " + t).header("Accept", "application/json");
            if (body != null) {
                b.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
            } else {
                b.method(method, HttpRequest.BodyPublishers.noBody());
            }
            HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
            int s = resp.statusCode();
            if (s == 401 && attempt == 0) {
                forget(t);
                continue;
            }
            if (s / 100 == 2) {
                return resp.body().isBlank() ? JSON.createObjectNode() : parse(resp.body());
            }
            JsonNode err = parse(resp.body()).path("error");
            String code = err.path("code").asText(""), msg = err.path("message").asText("");
            if (code.isEmpty() && err.isTextual()) {
                code = err.asText();
            }
            throw new ApiError(s, code, "Power BI answered " + s + (code.isEmpty() ? "" : " " + code) + (msg.isEmpty() ? "" : ": " + firstSentence(msg)),
                    resp.headers().firstValue("Retry-After").orElse(""));
        }
    }

    /**
     * The exception the worker acts on: a request Power BI cannot take (a
     * row the table does not fit, a dataset or table that is not there) is
     * invalid and not retried. Refused access and credentials are retried:
     * they recover once an administrator grants the access or the secret
     * is rotated. So are throttling (429) and Power BI's own failures.
     */
    static Exception classify(Exception e) {
        if (e instanceof ApiError a) {
            return switch (a.status) {
                case 400, 404, 413 -> new ConnectorException.Invalid(a.getMessage());
                case 401, 403 -> new IOException(a.getMessage()
                        + " (the service principal needs Member or Contributor access to the workspace, and the tenant must allow service principals to use Power BI APIs)", a);
                case 429 -> new IOException(a.getMessage() + (a.retryAfter.isEmpty() ? "" : "; retry after " + a.retryAfter + " s"), a);
                default -> a;
            };
        }
        if (e instanceof SignInError s) {
            return new IOException("Microsoft Entra refused the service principal: " + s.getMessage(), s);
        }
        return e;
    }

    private static JsonNode parse(String s) {
        try {
            JsonNode n = s == null || s.isBlank() ? null : JSON.readTree(s);
            return n == null ? JSON.createObjectNode() : n;
        } catch (IOException x) {
            return JSON.createObjectNode();
        }
    }

    private static String url(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    /** Entra's descriptions carry a trace and correlation ID after the first sentence. */
    static String firstSentence(String s) {
        int i = s.indexOf(". ");
        String out = (i > 0 ? s.substring(0, i + 1) : s).replaceAll("[\\r\\n]+", " ").trim();
        return out.length() > 300 ? out.substring(0, 300) + "…" : out;
    }
}
