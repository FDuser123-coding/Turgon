package dev.turgon.connector.powerbi.fake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A fake Power BI: the Microsoft Entra token endpoint (client credentials)
 * and the push dataset API of one workspace, kept in memory.
 *
 * <pre>
 * POST   /{tenant}/oauth2/v2.0/token
 * GET    /v1.0/myorg/groups/{workspace}/datasets
 * POST   /v1.0/myorg/groups/{workspace}/datasets?defaultRetentionPolicy=None   {"name", "defaultMode": "Push", "tables"}
 * GET    /v1.0/myorg/groups/{workspace}/datasets/{dataset}
 * GET    /v1.0/myorg/groups/{workspace}/datasets/{dataset}/tables
 * PUT    /v1.0/myorg/groups/{workspace}/datasets/{dataset}/tables/{table}  {"name", "columns"}
 * POST   /v1.0/myorg/groups/{workspace}/datasets/{dataset}/tables/{table}/rows   {"rows": [...]}
 * DELETE /v1.0/myorg/groups/{workspace}/datasets/{dataset}/tables/{table}/rows
 * </pre>
 *
 * Power BI has no API to read a push dataset's rows; the fake shows them at
 * {@code GET /fake/tables/{table}/rows} (the dataset it starts with) and
 * {@code GET /fake/datasets/{name}/tables/{table}/rows}. {@code POST /fake/throttle?n=2}
 * answers the next pushes with 429, {@code POST /fake/expire} revokes the
 * tokens issued so far.
 */
public final class FakePowerBi implements AutoCloseable {
    public static final int MAX_ROWS = 10_000;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern DATASET = Pattern.compile("/v1\\.0/myorg/groups/([^/]+)/datasets/([^/]+)(/tables(?:/([^/]+)(/rows)?)?)?");
    private static final Pattern DATASETS = Pattern.compile("/v1\\.0/myorg/groups/([^/]+)/datasets");
    private static final Set<String> TYPES = Set.of("Int64", "Double", "Decimal", "Boolean", "DateTime", "Datetime", "String");
    private static final Pattern FAKE_ROWS = Pattern.compile("/fake(?:/datasets/([^/]+))?/tables/([^/]+)/rows");

    private final HttpServer http;
    private final String tenant, clientId, clientSecret, workspace, dataset;
    /** The workspace's push datasets by ID: their names, and their tables' columns and rows. */
    private final Map<String, Dataset> datasets = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> tables;
    private final Map<String, List<JsonNode>> rows;
    private final Set<String> tokens = ConcurrentHashMap.newKeySet();
    private final AtomicInteger issued = new AtomicInteger(), throttle = new AtomicInteger(), pushes = new AtomicInteger();

    /**
     * @param tables each table's column names
     */
    public FakePowerBi(int port, String tenant, String clientId, String clientSecret, String workspace, String dataset,
            Map<String, Set<String>> tables) throws IOException {
        this.tenant = tenant;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.workspace = workspace;
        this.dataset = dataset;
        Dataset d = new Dataset("Sales", "None");
        tables.forEach(d::put);
        datasets.put(dataset, d);
        this.tables = d.tables;
        this.rows = d.rows;
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        http.createContext("/", this::handle);
        http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        http.start();
    }

    /** The base URL: both the authority and the API. */
    public String url() {
        return "http://127.0.0.1:" + http.getAddress().getPort();
    }

    /** A push dataset: its name, retention policy, tables' columns and rows. */
    static final class Dataset {
        final String name, retention;
        final Map<String, Set<String>> tables = new ConcurrentHashMap<>();
        final Map<String, List<JsonNode>> rows = new ConcurrentHashMap<>();

        Dataset(String name, String retention) {
            this.name = name;
            this.retention = retention;
        }

        void put(String table, Set<String> columns) {
            tables.put(table, new java.util.LinkedHashSet<>(columns));
            rows.computeIfAbsent(table, t -> java.util.Collections.synchronizedList(new ArrayList<>()));
        }
    }

    /** The rows pushed to a table of the first dataset, in order. */
    public List<JsonNode> rows(String table) {
        synchronized (rows.get(table)) {
            return List.copyOf(rows.get(table));
        }
    }

    /** The rows pushed to a table of the dataset named name. */
    public List<JsonNode> rows(String name, String table) {
        Dataset d = byName(name);
        List<JsonNode> r = d == null ? null : d.rows.get(table);
        if (r == null) {
            return List.of();
        }
        synchronized (r) {
            return List.copyOf(r);
        }
    }

    /** The columns of a table of the dataset named name, or null. */
    public Set<String> columns(String name, String table) {
        Dataset d = byName(name);
        return d == null ? null : d.tables.get(table);
    }

    /** The retention policy the dataset named name was created with, or null. */
    public String retention(String name) {
        Dataset d = byName(name);
        return d == null ? null : d.retention;
    }

    private Dataset byName(String name) {
        return datasets.values().stream().filter(d -> d.name.equals(name)).findFirst().orElse(null);
    }

    /** Answers the next n pushes with 429 Too Many Requests. */
    public void throttle(int n) {
        throttle.set(n);
    }

    /** Revokes every token issued so far, as if they had expired. */
    public void expireTokens() {
        tokens.clear();
    }

    /** How many tokens were issued. */
    public int tokensIssued() {
        return issued.get();
    }

    /** How many push requests were accepted. */
    public int pushes() {
        return pushes.get();
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath(), method = ex.getRequestMethod();
            if (method.equals("POST") && path.equals("/" + tenant + "/oauth2/v2.0/token")) {
                token(ex);
                return;
            }
            if (method.equals("POST") && path.equals("/fake/throttle")) {
                String q = ex.getRequestURI().getQuery();
                throttle(q != null && q.startsWith("n=") ? Integer.parseInt(q.substring(2)) : 1);
                send(ex, 204, null);
                return;
            }
            if (method.equals("POST") && path.equals("/fake/expire")) {
                expireTokens();
                send(ex, 204, null);
                return;
            }
            Matcher fr = FAKE_ROWS.matcher(path);
            if (method.equals("GET") && fr.matches()) {
                String table = decode(fr.group(2));
                Map<String, List<JsonNode>> in = rows;
                if (fr.group(1) != null) {
                    Dataset d = byName(decode(fr.group(1)));
                    in = d == null ? Map.of() : d.rows;
                }
                List<JsonNode> r = in.get(table);
                if (r == null) {
                    error(ex, 404, "ItemNotFound", "Table is not found");
                    return;
                }
                ObjectNode out = JSON.createObjectNode();
                synchronized (r) {
                    out.putArray("value").addAll(List.copyOf(r));
                }
                send(ex, 200, out);
                return;
            }
            Matcher all = DATASETS.matcher(path), m = DATASET.matcher(path);
            if (!all.matches() && !m.matches()) {
                error(ex, 404, "EntityNotFound", "Not found");
                return;
            }
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (auth == null || !auth.startsWith("Bearer ") || !tokens.contains(auth.substring(7))) {
                ex.getResponseHeaders().add("WWW-Authenticate", "Bearer error=\"invalid_token\"");
                error(ex, 401, "TokenExpired", "Access token has expired, resubmit with a new access token");
                return;
            }
            if (!decode((all.matches() ? all : m).group(1)).equals(workspace)) {
                error(ex, 404, "PowerBIEntityNotFound", "Workspace is not found or the caller has no access to it");
                return;
            }
            if (all.matches()) {
                datasets(ex, method);
                return;
            }
            Dataset ds = datasets.get(decode(m.group(2)));
            if (ds == null) {
                error(ex, 404, "ItemNotFound", "Dataset " + decode(m.group(2)) + " is not found");
                return;
            }
            if (m.group(3) == null && method.equals("GET")) {
                ObjectNode d = JSON.createObjectNode().put("id", decode(m.group(2))).put("name", ds.name).put("addRowsAPIEnabled", true)
                        .put("configuredBy", clientId).put("isRefreshable", false);
                send(ex, 200, d);
                return;
            }
            if (m.group(4) == null && method.equals("GET")) {
                ObjectNode out = JSON.createObjectNode();
                ArrayNode value = out.putArray("value");
                ds.tables.keySet().stream().sorted().forEach(t -> value.addObject().put("name", t));
                send(ex, 200, out);
                return;
            }
            String table = m.group(4) == null ? null : decode(m.group(4));
            if (table != null && m.group(5) == null && method.equals("PUT")) {
                putTable(ex, ds, table);
                return;
            }
            if (table == null || m.group(5) == null || !ds.tables.containsKey(table)) {
                error(ex, 404, "ItemNotFound", "Table " + table + " is not found in the dataset");
                return;
            }
            Map<String, Set<String>> tables = ds.tables;
            Map<String, List<JsonNode>> rows = ds.rows;
            switch (method) {
                case "POST" -> push(ex, ds, table);
                case "DELETE" -> {
                    rows.get(table).clear();
                    send(ex, 200, null);
                }
                default -> error(ex, 405, "MethodNotAllowed", "Method not allowed");
            }
        } catch (RuntimeException e) {
            error(ex, 500, "InternalServerError", e.getClass().getSimpleName());
        }
    }

    /** Lists the workspace's datasets, or creates a push dataset. */
    private void datasets(HttpExchange ex, String method) throws IOException {
        if (method.equals("GET")) {
            ObjectNode out = JSON.createObjectNode();
            ArrayNode value = out.putArray("value");
            datasets.forEach((id, d) -> value.addObject().put("id", id).put("name", d.name).put("addRowsAPIEnabled", true));
            send(ex, 200, out);
            return;
        }
        if (!method.equals("POST")) {
            error(ex, 405, "MethodNotAllowed", "Method not allowed");
            return;
        }
        String query = ex.getRequestURI().getQuery() == null ? "" : ex.getRequestURI().getQuery();
        String retention = query.contains("defaultRetentionPolicy=basicFIFO") ? "basicFIFO" : "None";
        JsonNode body = JSON.readTree(ex.getRequestBody());
        String name = body == null ? "" : body.path("name").asText("");
        if (name.isEmpty() || !"Push".equals(body.path("defaultMode").asText("Push"))) {
            error(ex, 400, "InvalidRequest", "A push dataset needs a name and defaultMode Push");
            return;
        }
        Dataset d = new Dataset(name, retention);
        for (JsonNode t : body.path("tables")) {
            Set<String> cols = columns(t);
            if (cols == null) {
                error(ex, 400, "InvalidRequest", "Table " + t.path("name").asText() + " has a column without a name or a known dataType");
                return;
            }
            d.put(t.path("name").asText(), cols);
        }
        if (d.tables.isEmpty()) {
            error(ex, 400, "InvalidRequest", "A push dataset needs at least one table");
            return;
        }
        String id = UUID.randomUUID().toString();
        datasets.put(id, d);
        send(ex, 201, JSON.createObjectNode().put("id", id).put("name", name).put("defaultRetentionPolicy", retention));
    }

    /** Adds a table to a push dataset, or replaces its columns. */
    private void putTable(HttpExchange ex, Dataset d, String table) throws IOException {
        JsonNode body = JSON.readTree(ex.getRequestBody());
        Set<String> cols = body == null ? null : columns(body);
        if (cols == null || !table.equals(body.path("name").asText(table))) {
            error(ex, 400, "InvalidRequest", "The table needs its name and columns, each with a name and a known dataType");
            return;
        }
        d.put(table, cols);
        send(ex, 200, body);
    }

    private static Set<String> columns(JsonNode t) {
        Set<String> cols = new java.util.LinkedHashSet<>();
        for (JsonNode c : t.path("columns")) {
            if (c.path("name").asText("").isEmpty() || !TYPES.contains(c.path("dataType").asText())) {
                return null;
            }
            cols.add(c.path("name").asText());
        }
        return cols.isEmpty() || t.path("name").asText("").isEmpty() ? null : cols;
    }

    private void push(HttpExchange ex, Dataset ds, String table) throws IOException {
        Map<String, Set<String>> tables = ds.tables;
        Map<String, List<JsonNode>> rows = ds.rows;
        if (throttle.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            ex.getResponseHeaders().add("Retry-After", "1");
            error(ex, 429, "TooManyRequests", "Rate limit exceeded");
            return;
        }
        JsonNode body;
        try {
            body = JSON.readTree(ex.getRequestBody());
        } catch (IOException e) {
            error(ex, 400, "InvalidRequest", "The request body is not JSON");
            return;
        }
        JsonNode in = body == null ? null : body.get("rows");
        if (in == null || !in.isArray()) {
            error(ex, 400, "InvalidRequest", "The request body must be {\"rows\": [...]}");
            return;
        }
        if (in.size() > MAX_ROWS) {
            error(ex, 400, "InvalidRequest", "A request can add at most " + MAX_ROWS + " rows");
            return;
        }
        Set<String> cols = tables.get(table);
        for (JsonNode r : in) {
            if (!r.isObject()) {
                error(ex, 400, "InvalidRequest", "Each row must be an object");
                return;
            }
            for (var it = r.fieldNames(); it.hasNext();) {
                String c = it.next();
                if (!cols.contains(c)) {
                    error(ex, 400, "InvalidRequest", "Column '" + c + "' is not found in table '" + table + "'");
                    return;
                }
            }
        }
        in.forEach(rows.get(table)::add);
        pushes.incrementAndGet();
        send(ex, 200, null);
    }

    private void token(HttpExchange ex) throws IOException {
        Map<String, String> form = new HashMap<>();
        for (String kv : new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) {
                form.put(decode(kv.substring(0, i)), decode(kv.substring(i + 1)));
            }
        }
        if (!"client_credentials".equals(form.get("grant_type"))) {
            entraError(ex, 400, "unsupported_grant_type", "AADSTS70003: The app requested an unsupported grant type.");
        } else if (!clientId.equals(form.get("client_id"))) {
            entraError(ex, 400, "unauthorized_client", "AADSTS700016: Application with identifier '" + form.get("client_id")
                    + "' was not found in the directory '" + tenant + "'.");
        } else if (!clientSecret.equals(form.get("client_secret"))) {
            entraError(ex, 401, "invalid_client", "AADSTS7000215: Invalid client secret provided. Ensure the secret being sent in the request"
                    + " is the client secret value, not the client secret ID.");
        } else if (!"https://analysis.windows.net/powerbi/api/.default".equals(form.get("scope"))) {
            entraError(ex, 400, "invalid_scope", "AADSTS70011: The provided request must include a 'scope' input parameter.");
        } else {
            String t = UUID.randomUUID().toString();
            tokens.add(t);
            issued.incrementAndGet();
            send(ex, 200, JSON.createObjectNode().put("token_type", "Bearer").put("expires_in", 3599).put("access_token", t));
        }
    }

    private static void entraError(HttpExchange ex, int status, String code, String description) throws IOException {
        send(ex, status, JSON.createObjectNode().put("error", code).put("error_description", description));
    }

    private static void error(HttpExchange ex, int status, String code, String message) throws IOException {
        ObjectNode e = JSON.createObjectNode();
        e.putObject("error").put("code", code).put("message", message);
        send(ex, status, e);
    }

    private static void send(HttpExchange ex, int status, JsonNode body) throws IOException {
        byte[] b = body == null ? new byte[0] : JSON.writeValueAsBytes(body);
        if (body != null) {
            ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        }
        ex.sendResponseHeaders(status, b.length == 0 ? -1 : b.length);
        if (b.length > 0) {
            ex.getResponseBody().write(b);
        }
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    /**
     * Runs the fake: {@code --port 9400 --tenant T --client-id C --client-secret S --workspace W --dataset D
     * --table SalesOrders=OrderId,Customer,...} (the table flag repeats).
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> flags = new HashMap<>();
        Map<String, Set<String>> tables = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].equals("--table")) {
                String[] kv = args[i + 1].split("=", 2);
                tables.put(kv[0], new java.util.LinkedHashSet<>(List.of(kv[1].split(","))));
            } else {
                flags.put(args[i].replaceFirst("^--", ""), args[i + 1]);
            }
        }
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("--table name=column,column,... is required");
        }
        FakePowerBi f = new FakePowerBi(Integer.parseInt(flags.getOrDefault("port", "9400")), flags.getOrDefault("tenant", "contoso"),
                flags.getOrDefault("client-id", "turgon"), flags.getOrDefault("client-secret", "secret"),
                flags.getOrDefault("workspace", "sales"), flags.getOrDefault("dataset", "orders"), tables);
        System.out.println("fake Power BI listening on " + f.url() + ": tenant " + f.tenant + ", workspace " + f.workspace
                + ", dataset " + f.dataset + ", tables " + tables.keySet());
        Thread.currentThread().join();
    }
}
