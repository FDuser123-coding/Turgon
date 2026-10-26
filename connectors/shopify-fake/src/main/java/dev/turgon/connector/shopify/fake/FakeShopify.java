package dev.turgon.connector.shopify.fake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A fake Shopify store: the GraphQL Admin API ({@code POST
 * /admin/api/{version}/graphql.json}) for the operations the connector
 * sends, kept in memory. Requests need the access token; each operation
 * needs its access scope, as Shopify's do. The fake answers by operation
 * name rather than by parsing the query.
 *
 * <p>For tests and demos: {@code POST /fake/orders} adds an order (a
 * simplified REST-style body), {@code GET /fake/orders/{id}} shows one,
 * {@code POST /fake/reject-update?order=ID} makes the next update of that
 * order fail with a user error, {@code POST /fake/throttle?n=1} throttles
 * the next queries.
 */
public final class FakeShopify implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern GRAPHQL = Pattern.compile("/admin/api/([^/]+)/graphql\\.json");
    private static final Pattern FAKE_ORDER = Pattern.compile("/fake/orders/(\\d+)");
    private static final Pattern ID_AFTER = Pattern.compile("id:>(\\d+)");

    private final HttpServer http;
    private final String shop, token, latest;
    private final Set<String> versions, scopes;
    private final Map<Long, ObjectNode> orders = new ConcurrentHashMap<>();
    private final Map<Long, ObjectNode> customers = new ConcurrentHashMap<>();
    private final Set<Long> rejectUpdate = ConcurrentHashMap.newKeySet();
    private final AtomicLong nextOrder = new AtomicLong(5_600_000_000_001L), nextCustomer = new AtomicLong(7_200_000_000_001L);
    private final AtomicInteger number = new AtomicInteger(1001), throttle = new AtomicInteger(), updates = new AtomicInteger();

    /**
     * @param versions the API versions the store still serves; others fall forward to {@code latest}
     */
    public FakeShopify(int port, String shop, String token, Set<String> versions, String latest, Set<String> scopes) throws IOException {
        this.shop = shop;
        this.token = token;
        this.versions = Set.copyOf(versions);
        this.latest = latest;
        this.scopes = Set.copyOf(scopes);
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        http.createContext("/", this::handle);
        http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        http.start();
    }

    public String url() {
        return "http://127.0.0.1:" + http.getAddress().getPort();
    }

    /** Throttles the next n GraphQL requests, as Shopify does when the cost bucket is empty. */
    public void throttle(int n) {
        throttle.set(n);
    }

    /** The next update of the order fails with a user error. */
    public void rejectUpdate(long order) {
        rejectUpdate.add(order);
    }

    /** How many orderUpdate mutations succeeded. */
    public int updates() {
        return updates.get();
    }

    /** The order as the GraphQL API holds it, or null. */
    public ObjectNode order(long id) {
        ObjectNode o = orders.get(id);
        return o == null ? null : o.deepCopy();
    }

    /**
     * Adds an order: {"email", "currency", "subtotal", "created_at"?, "note"?,
     * "customer": {"email", "first_name", "last_name"}, "shipping_code"?,
     * "line_items": [{"sku", "name", "quantity", "price"}]}. Returns its numeric ID.
     */
    public synchronized long addOrder(JsonNode in) {
        long id = nextOrder.getAndIncrement();
        ObjectNode o = JSON.createObjectNode();
        o.put("id", "gid://shopify/Order/" + id).put("legacyResourceId", Long.toString(id)).put("name", "#" + number.getAndIncrement());
        String created = in.path("created_at").asText(Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
        o.put("createdAt", created).put("updatedAt", created).put("email", in.path("email").asText(in.path("customer").path("email").asText("")));
        String cur = in.path("currency").asText("EUR");
        o.put("currencyCode", cur);
        if (in.hasNonNull("note")) {
            o.put("note", in.get("note").asText());
        } else {
            o.putNull("note");
        }
        o.putArray("tags");
        o.put("displayFinancialStatus", "PAID");
        String subtotal = in.path("subtotal").asText("0.00");
        money(o, "subtotalPriceSet", subtotal, cur);
        money(o, "totalPriceSet", subtotal, cur);
        money(o, "totalTaxSet", "0.00", cur);
        JsonNode c = in.path("customer");
        if (c.isObject()) {
            long cid = customers.values().stream().filter(x -> x.path("defaultEmailAddress").path("emailAddress").asText().equals(c.path("email").asText()))
                    .map(x -> Long.parseLong(x.path("legacyResourceId").asText())).findFirst().orElseGet(() -> {
                        long n = nextCustomer.getAndIncrement();
                        ObjectNode cu = JSON.createObjectNode().put("id", "gid://shopify/Customer/" + n).put("legacyResourceId", Long.toString(n))
                                .put("firstName", c.path("first_name").asText("")).put("lastName", c.path("last_name").asText(""));
                        cu.putObject("defaultEmailAddress").put("emailAddress", c.path("email").asText());
                        customers.put(n, cu);
                        return n;
                    });
            o.set("customer", customers.get(cid).deepCopy());
        } else {
            o.putNull("customer");
        }
        ArrayNode ship = o.putObject("shippingLines").putArray("nodes");
        if (in.hasNonNull("shipping_code")) {
            ObjectNode s = ship.addObject().put("code", in.get("shipping_code").asText()).put("title", in.get("shipping_code").asText());
            money(s, "originalPriceSet", "0.00", cur);
        }
        ArrayNode lines = o.putObject("lineItems").putArray("nodes");
        int li = 1;
        for (JsonNode l : in.path("line_items")) {
            ObjectNode n = lines.addObject().put("id", "gid://shopify/LineItem/" + id + li++).put("sku", l.path("sku").asText())
                    .put("name", l.path("name").asText(l.path("sku").asText())).put("quantity", l.path("quantity").asInt(1));
            money(n, "originalUnitPriceSet", l.path("price").asText("0.00"), cur);
        }
        orders.put(id, o);
        return id;
    }

    private static void money(ObjectNode parent, String field, String amount, String currency) {
        parent.putObject(field).putObject("shopMoney").put("amount", amount).put("currencyCode", currency);
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath(), method = ex.getRequestMethod(), query = ex.getRequestURI().getQuery();
            if (path.startsWith("/fake/")) {
                fake(ex, method, path, query);
                return;
            }
            Matcher m = GRAPHQL.matcher(path);
            if (!method.equals("POST") || !m.matches()) {
                send(ex, 404, JSON.createObjectNode().put("errors", "Not Found"));
                return;
            }
            String version = versions.contains(m.group(1)) ? m.group(1) : latest;
            ex.getResponseHeaders().add("X-Shopify-API-Version", version);
            if (!token.equals(ex.getRequestHeaders().getFirst("X-Shopify-Access-Token"))) {
                send(ex, 401, JSON.createObjectNode().put("errors", "[API] Invalid API key or access token (unrecognized login or wrong password)"));
                return;
            }
            JsonNode req = JSON.readTree(ex.getRequestBody());
            if (throttle.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                ObjectNode out = JSON.createObjectNode();
                out.putArray("errors").addObject().put("message", "Throttled").putObject("extensions").put("code", "THROTTLED");
                cost(out, 0);
                send(ex, 200, out);
                return;
            }
            send(ex, 200, graphql(req.path("operationName").asText(""), req.path("variables")));
        } catch (RuntimeException e) {
            send(ex, 500, JSON.createObjectNode().put("errors", "Internal Server Error"));
        }
    }

    private static void cost(ObjectNode out, double available) {
        out.putObject("extensions").putObject("cost").put("requestedQueryCost", 10).putObject("throttleStatus")
                .put("maximumAvailable", 2000).put("currentlyAvailable", available).put("restoreRate", 100);
    }

    private ObjectNode denied(String scope) {
        ObjectNode out = JSON.createObjectNode();
        out.putArray("errors").addObject().put("message", "Access denied for this field. Required access: `" + scope + "` access scope.")
                .putObject("extensions").put("code", "ACCESS_DENIED");
        return out;
    }

    private ObjectNode graphql(String op, JsonNode vars) {
        ObjectNode out = JSON.createObjectNode();
        ObjectNode data = out.putObject("data");
        switch (op) {
            case "Shop" -> data.putObject("shop").put("name", "Turgon Demo").put("myshopifyDomain", shop).put("currencyCode", "EUR");
            case "Scopes" -> {
                ArrayNode a = data.putObject("currentAppInstallation").putArray("accessScopes");
                new java.util.TreeSet<>(scopes).forEach(s -> a.addObject().put("handle", s));
            }
            case "Orders" -> {
                if (!scopes.contains("read_orders")) {
                    return denied("read_orders");
                }
                Matcher m = ID_AFTER.matcher(vars.path("query").asText(""));
                long after = m.find() ? Long.parseLong(m.group(1)) : 0;
                int first = Math.min(250, vars.path("first").asInt(50));
                ArrayNode nodes = data.putObject("orders").putArray("nodes");
                new TreeMap<>(orders).tailMap(after, false).values().stream().limit(first).forEach(nodes::add);
            }
            case "Order" -> {
                if (!scopes.contains("read_orders")) {
                    return denied("read_orders");
                }
                ObjectNode o = orders.get(legacy(vars.path("id").asText()));
                data.set("order", o == null ? JSON.nullNode() : o);
            }
            case "Customer" -> {
                if (!scopes.contains("read_customers")) {
                    return denied("read_customers");
                }
                ObjectNode c = customers.get(legacy(vars.path("id").asText()));
                data.set("customer", c == null ? JSON.nullNode() : c);
            }
            case "OrderUpdate" -> {
                if (!scopes.contains("write_orders")) {
                    return denied("write_orders");
                }
                JsonNode in = vars.path("input");
                long id = legacy(in.path("id").asText());
                ObjectNode res = data.putObject("orderUpdate");
                ArrayNode errs = res.putArray("userErrors");
                ObjectNode o = orders.get(id);
                if (o == null) {
                    errs.addObject().put("message", "Order does not exist").putArray("field").add("id");
                    res.putNull("order");
                } else if (rejectUpdate.remove(id)) {
                    errs.addObject().put("message", "Order can't be edited: it is archived").putArray("field").add("id");
                    res.putNull("order");
                } else {
                    synchronized (o) {
                        if (in.has("note")) {
                            o.set("note", in.get("note"));
                        }
                        if (in.has("tags")) {
                            o.set("tags", in.get("tags"));
                        }
                        o.put("updatedAt", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
                    }
                    updates.incrementAndGet();
                    res.putObject("order").put("id", o.get("id").asText()).set("note", o.get("note"));
                    ((ObjectNode) res.get("order")).set("tags", o.get("tags"));
                }
            }
            case "Type" -> data.set("__type", type(vars.path("name").asText()));
            default -> {
                out.remove("data");
                out.putArray("errors").addObject().put("message", "the fake does not answer operation " + op);
                return out;
            }
        }
        cost(out, 1990);
        return out;
    }

    private static long legacy(String id) {
        String s = id.substring(id.lastIndexOf('/') + 1);
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException x) {
            return -1;
        }
    }

    /** A little of Shopify's schema, for introspection. */
    private static JsonNode type(String name) {
        Map<String, List<String[]>> schema = Map.of(
                "Order", List.of(f("id", "NON_NULL", "ID"), f("legacyResourceId", "NON_NULL", "UnsignedInt64"), f("name", "NON_NULL", "String"),
                        f("createdAt", "NON_NULL", "DateTime"), f("updatedAt", "NON_NULL", "DateTime"), f("email", "SCALAR", "String"),
                        f("currencyCode", "NON_NULL", "CurrencyCode"), f("note", "SCALAR", "String"), f("tags", "NON_NULL", "String"),
                        f("displayFinancialStatus", "ENUM", "OrderDisplayFinancialStatus"), f("subtotalPriceSet", "OBJECT", "MoneyBag"),
                        f("totalPriceSet", "NON_NULL", "MoneyBag"), f("totalTaxSet", "OBJECT", "MoneyBag"), f("customer", "OBJECT", "Customer"),
                        f("shippingLines", "NON_NULL", "ShippingLineConnection"), f("lineItems", "NON_NULL", "LineItemConnection")),
                "Customer", List.of(f("id", "NON_NULL", "ID"), f("legacyResourceId", "NON_NULL", "UnsignedInt64"), f("firstName", "SCALAR", "String"),
                        f("lastName", "SCALAR", "String"), f("defaultEmailAddress", "OBJECT", "CustomerEmailAddress")));
        List<String[]> fields = schema.get(name);
        if (fields == null) {
            return JSON.nullNode();
        }
        ObjectNode t = JSON.createObjectNode().put("name", name).put("kind", "OBJECT");
        ArrayNode fs = t.putArray("fields");
        for (String[] f : fields) {
            ObjectNode type = fs.addObject().put("name", f[0]).putObject("type");
            if (f[1].equals("NON_NULL")) {
                type.put("kind", "NON_NULL").putNull("name");
                type.putObject("ofType").put("kind", f[2].equals("String") || f[2].equals("ID") || f[2].equals("DateTime")
                        || f[2].equals("UnsignedInt64") ? "SCALAR" : "OBJECT").put("name", f[2]);
            } else {
                type.put("kind", f[1]).put("name", f[2]);
            }
        }
        return t;
    }

    private static String[] f(String name, String kind, String type) {
        return new String[] {name, kind, type};
    }

    private void fake(HttpExchange ex, String method, String path, String query) throws IOException {
        Map<String, String> q = new HashMap<>();
        if (query != null) {
            for (String kv : query.split("&")) {
                String[] p = kv.split("=", 2);
                q.put(p[0], p.length > 1 ? p[1] : "");
            }
        }
        Matcher m = FAKE_ORDER.matcher(path);
        if (method.equals("POST") && path.equals("/fake/orders")) {
            long id = addOrder(JSON.readTree(ex.getRequestBody()));
            send(ex, 201, orders.get(id));
        } else if (method.equals("GET") && m.matches()) {
            ObjectNode o = orders.get(Long.parseLong(m.group(1)));
            send(ex, o == null ? 404 : 200, o == null ? JSON.createObjectNode().put("errors", "Not Found") : o);
        } else if (method.equals("POST") && path.equals("/fake/reject-update")) {
            rejectUpdate(Long.parseLong(q.getOrDefault("order", "0")));
            send(ex, 204, null);
        } else if (method.equals("POST") && path.equals("/fake/throttle")) {
            throttle(Integer.parseInt(q.getOrDefault("n", "1")));
            send(ex, 204, null);
        } else {
            send(ex, 404, JSON.createObjectNode().put("errors", "Not Found"));
        }
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

    /**
     * Runs the fake: {@code --port 9500 --shop turgon-demo.myshopify.com --token shpat_demo
     * --versions 2026-07,2026-10 --scopes read_orders,write_orders,read_customers}.
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> flags = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            flags.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        List<String> versions = List.of(flags.getOrDefault("versions", "2026-07,2026-10").split(","));
        FakeShopify f = new FakeShopify(Integer.parseInt(flags.getOrDefault("port", "9500")), flags.getOrDefault("shop", "turgon-demo.myshopify.com"),
                flags.getOrDefault("token", "shpat_demo"), Set.copyOf(versions), versions.get(versions.size() - 1),
                Set.of(flags.getOrDefault("scopes", "read_orders,write_orders,read_customers").split(",")));
        System.out.println("fake Shopify listening on " + f.url() + ": store " + f.shop + ", API versions " + versions + ", scopes " + f.scopes);
        Thread.currentThread().join();
    }
}
