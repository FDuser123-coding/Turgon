package dev.turgon.connector.stripe.fake;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 * A fake Stripe account: the parts of the API the connector uses, kept in
 * memory, with Stripe's conventions: form-encoded requests, a Bearer
 * secret or restricted key, idempotency keys, permissions per restricted
 * key, and events listed newest first with {@code starting_after} paging.
 *
 * <p>For tests and demos: {@code POST /fake/customers} (email, name) adds a
 * customer, {@code POST /fake/pay?invoice=in_...} pays an open invoice and
 * records its {@code invoice.paid} event, {@code GET /fake/invoices/{id}}
 * shows an invoice, {@code POST /fake/throttle?n=1} rate-limits the next
 * requests.
 */
public final class FakeStripe implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern INVOICE = Pattern.compile("/v1/invoices/(in_[A-Za-z0-9]+)(/finalize|/void)?");
    private static final Pattern CUSTOMER = Pattern.compile("/v1/customers/(cus_[A-Za-z0-9]+)");
    private static final Pattern FAKE_INVOICE = Pattern.compile("/fake/invoices/(in_[A-Za-z0-9]+)");

    private final HttpServer http;
    private final String key;
    private final Set<String> permissions;
    private final Map<String, ObjectNode> customers = new ConcurrentHashMap<>(), invoices = new ConcurrentHashMap<>();
    private final List<ObjectNode> events = java.util.Collections.synchronizedList(new ArrayList<>());
    private final Map<String, String[]> idempotent = new ConcurrentHashMap<>(); // key -> {request, status, response}
    private final AtomicLong seq = new AtomicLong(1000);
    private final AtomicInteger number = new AtomicInteger(1), throttle = new AtomicInteger();
    private long lastEvent;

    /**
     * @param permissions what the key may do: {@code customers:read}, {@code invoices:read},
     *     {@code invoices:write}, {@code events:read}; null for a full secret key
     */
    public FakeStripe(int port, String key, Set<String> permissions) throws IOException {
        this.key = key;
        this.permissions = permissions;
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        http.createContext("/", this::handle);
        http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        http.start();
    }

    public String url() {
        return "http://127.0.0.1:" + http.getAddress().getPort();
    }

    public void throttle(int n) {
        throttle.set(n);
    }

    /** Adds a customer and returns its ID. */
    public String addCustomer(String email, String name) {
        String id = "cus_" + seq.incrementAndGet();
        ObjectNode c = JSON.createObjectNode().put("id", id).put("object", "customer").put("email", email).put("name", name)
                .put("created", Instant.now().getEpochSecond()).put("livemode", false);
        c.putObject("metadata");
        customers.put(id, c);
        return id;
    }

    public ObjectNode invoice(String id) {
        ObjectNode i = invoices.get(id);
        return i == null ? null : i.deepCopy();
    }

    /** All invoices that were not deleted. */
    public List<ObjectNode> invoices() {
        return invoices.values().stream().map(ObjectNode::deepCopy).toList();
    }

    /** Pays an open invoice and records its invoice.paid event. */
    public synchronized ObjectNode pay(String id) {
        ObjectNode inv = invoices.get(id);
        if (inv == null || !inv.path("status").asText().equals("open")) {
            throw new IllegalStateException("invoice " + id + " is not open");
        }
        long now = Math.max(Instant.now().getEpochSecond(), lastEvent + 1); // events in strictly increasing seconds
        lastEvent = now;
        inv.put("status", "paid").put("amount_paid", inv.path("total").asLong()).put("amount_remaining", 0).put("paid", true);
        ((ObjectNode) inv.get("status_transitions")).put("paid_at", now);
        ObjectNode ev = JSON.createObjectNode().put("id", "evt_" + seq.incrementAndGet()).put("object", "event").put("type", "invoice.paid")
                .put("created", now).put("api_version", "2026-06-30.basil").put("livemode", false);
        ev.putObject("data").set("object", inv.deepCopy());
        events.add(ev);
        return ev;
    }

    @Override
    public void close() {
        http.stop(0);
    }

    private static Map<String, String> form(String body) {
        Map<String, String> out = new LinkedHashMap<>();
        if (body == null || body.isEmpty()) {
            return out;
        }
        for (String kv : body.split("&")) {
            int i = kv.indexOf('=');
            String k = URLDecoder.decode(i < 0 ? kv : kv.substring(0, i), StandardCharsets.UTF_8);
            out.put(k, i < 0 ? "" : URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    private record Reply(int status, JsonNode body) {}

    private static Reply error(int status, String type, String code, String message, String param) {
        ObjectNode e = JSON.createObjectNode();
        ObjectNode err = e.putObject("error").put("type", type).put("message", message);
        if (code != null) {
            err.put("code", code);
        }
        if (param != null) {
            err.put("param", param);
        }
        return new Reply(status, e);
    }

    private Reply missing(String what, String id, String param) {
        return error(404, "invalid_request_error", "resource_missing", "No such " + what + ": '" + id + "'", param);
    }

    private Reply denied(String permission) {
        return error(403, "invalid_request_error", null, "The provided key '" + key.substring(0, 8) + "***" + key.substring(key.length() - 4)
                + "' does not have the required permissions for this endpoint on account 'acct_fake'. Having the '" + permission
                + "' permission would allow this request to continue.", null);
    }

    private boolean may(String permission) {
        return permissions == null || permissions.contains(permission);
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getPath(), method = ex.getRequestMethod();
            Map<String, String> query = form(ex.getRequestURI().getRawQuery());
            Map<String, String> body = form(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            if (path.startsWith("/fake/")) {
                Reply r = fake(method, path, query, body);
                send(ex, r);
                return;
            }
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (auth == null || !auth.equals("Bearer " + key)) {
                send(ex, error(401, "invalid_request_error", null, "Invalid API Key provided: " + (auth == null ? "none" : "***"), null));
                return;
            }
            if (throttle.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                ex.getResponseHeaders().add("Stripe-Should-Retry", "true");
                send(ex, error(429, "invalid_request_error", "rate_limit", "Request rate limit exceeded.", null));
                return;
            }
            String idem = ex.getRequestHeaders().getFirst("Idempotency-Key");
            if (method.equals("POST") && idem != null) {
                String req = method + " " + path + " " + new TreeMap<>(body);
                String[] prior = idempotent.get(idem);
                if (prior != null) {
                    if (!prior[0].equals(req)) {
                        send(ex, error(400, "idempotency_error", null,
                                "Keys for idempotent requests can only be used with the same parameters they were first used with.", null));
                        return;
                    }
                    ex.getResponseHeaders().add("Idempotent-Replayed", "true");
                    send(ex, new Reply(Integer.parseInt(prior[1]), JSON.readTree(prior[2])));
                    return;
                }
                Reply r = api(method, path, query, body);
                idempotent.put(idem, new String[] {req, Integer.toString(r.status), JSON.writeValueAsString(r.body)});
                send(ex, r);
                return;
            }
            send(ex, api(method, path, query, body));
        } catch (RuntimeException e) {
            send(ex, error(500, "api_error", null, "An unknown error occurred", null));
        }
    }

    private synchronized Reply api(String method, String path, Map<String, String> q, Map<String, String> b) {
        Matcher mi = INVOICE.matcher(path), mc = CUSTOMER.matcher(path);
        switch (method + " " + (mi.matches() ? "/v1/invoices/{id}" + (mi.group(2) == null ? "" : mi.group(2)) : mc.matches() ? "/v1/customers/{id}" : path)) {
            case "GET /v1/customers" -> {
                return may("customers:read") ? list(customers.values(), q) : denied("rak_customer_read");
            }
            case "GET /v1/customers/{id}" -> {
                if (!may("customers:read")) {
                    return denied("rak_customer_read");
                }
                ObjectNode c = customers.get(mc.group(1));
                return c == null ? missing("customer", mc.group(1), "id") : new Reply(200, c);
            }
            case "GET /v1/invoices" -> {
                return may("invoices:read") ? list(invoices.values(), q) : denied("rak_invoice_read");
            }
            case "GET /v1/invoices/{id}" -> {
                if (!may("invoices:read")) {
                    return denied("rak_invoice_read");
                }
                ObjectNode i = invoices.get(mi.group(1));
                return i == null ? missing("invoice", mi.group(1), "id") : new Reply(200, i);
            }
            case "GET /v1/events" -> {
                if (!may("events:read")) {
                    return denied("rak_event_read");
                }
                return events(q);
            }
            case "POST /v1/invoices" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                String cus = b.getOrDefault("customer", "");
                if (!customers.containsKey(cus)) {
                    return error(400, "invalid_request_error", "resource_missing", "No such customer: '" + cus + "'", "customer");
                }
                String id = "in_" + seq.incrementAndGet();
                ObjectNode inv = JSON.createObjectNode().put("id", id).put("object", "invoice").put("customer", cus)
                        .put("customer_email", customers.get(cus).path("email").asText()).put("status", "draft")
                        .put("currency", b.getOrDefault("currency", "eur")).put("collection_method", b.getOrDefault("collection_method", "charge_automatically"))
                        .put("subtotal", 0).put("total", 0).put("amount_due", 0).put("amount_paid", 0).put("amount_remaining", 0)
                        .put("created", Instant.now().getEpochSecond()).put("livemode", false).put("paid", false);
                inv.putNull("number");
                inv.putObject("status_transitions").putNull("paid_at");
                inv.putObject("lines").put("object", "list").putArray("data");
                ObjectNode md = inv.putObject("metadata");
                b.forEach((k, v) -> {
                    if (k.startsWith("metadata[") && k.endsWith("]")) {
                        md.put(k.substring(9, k.length() - 1), v);
                    }
                });
                if (b.containsKey("days_until_due")) {
                    inv.put("due_date", Instant.now().getEpochSecond() + Long.parseLong(b.get("days_until_due")) * 86400);
                }
                invoices.put(id, inv);
                return new Reply(200, inv);
            }
            case "POST /v1/invoiceitems" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                ObjectNode inv = invoices.get(b.getOrDefault("invoice", ""));
                if (inv == null) {
                    return error(400, "invalid_request_error", "resource_missing", "No such invoice: '" + b.get("invoice") + "'", "invoice");
                }
                if (!inv.path("status").asText().equals("draft")) {
                    return error(400, "invalid_request_error", "invoice_not_editable", "You can only add invoice items to draft invoices.", "invoice");
                }
                long amount;
                try {
                    amount = Long.parseLong(b.getOrDefault("amount", ""));
                } catch (NumberFormatException x) {
                    return error(400, "invalid_request_error", "parameter_invalid_integer", "Invalid integer: " + b.get("amount"), "amount");
                }
                ObjectNode li = JSON.createObjectNode().put("id", "ii_" + seq.incrementAndGet()).put("object", "invoiceitem").put("amount", amount)
                        .put("currency", b.getOrDefault("currency", inv.path("currency").asText())).put("description", b.getOrDefault("description", ""))
                        .put("invoice", inv.path("id").asText());
                ((ArrayNode) inv.get("lines").get("data")).add(li);
                long total = inv.path("total").asLong() + amount;
                inv.put("subtotal", total).put("total", total).put("amount_due", total).put("amount_remaining", total);
                return new Reply(200, li);
            }
            case "POST /v1/invoices/{id}/finalize" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                ObjectNode inv = invoices.get(mi.group(1));
                if (inv == null) {
                    return missing("invoice", mi.group(1), "invoice");
                }
                if (!inv.path("status").asText().equals("draft")) {
                    return error(400, "invalid_request_error", "invoice_already_finalized", "This invoice is already finalized.", null);
                }
                inv.put("status", "open").put("number", String.format("TURG-%04d", number.getAndIncrement()));
                return new Reply(200, inv);
            }
            case "POST /v1/invoices/{id}/void" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                ObjectNode inv = invoices.get(mi.group(1));
                if (inv == null) {
                    return missing("invoice", mi.group(1), "invoice");
                }
                if (!inv.path("status").asText().equals("open")) {
                    return error(400, "invalid_request_error", "invoice_not_open", "You can only void open invoices; this one is " + inv.path("status").asText() + ".", null);
                }
                inv.put("status", "void");
                return new Reply(200, inv);
            }
            case "DELETE /v1/invoices/{id}" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                ObjectNode inv = invoices.get(mi.group(1));
                if (inv == null) {
                    return missing("invoice", mi.group(1), "id");
                }
                if (!inv.path("status").asText().equals("draft")) {
                    return error(400, "invalid_request_error", null, "You can only delete draft invoices.", null);
                }
                invoices.remove(mi.group(1));
                return new Reply(200, JSON.createObjectNode().put("id", mi.group(1)).put("object", "invoice").put("deleted", true));
            }
            case "POST /v1/invoices/{id}" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                ObjectNode inv = invoices.get(mi.group(1));
                if (inv == null) {
                    return missing("invoice", mi.group(1), "id");
                }
                ObjectNode md = (ObjectNode) inv.get("metadata");
                b.forEach((k, v) -> {
                    if (k.startsWith("metadata[") && k.endsWith("]")) {
                        String mk = k.substring(9, k.length() - 1);
                        if (v.isEmpty()) {
                            md.remove(mk); // Stripe deletes a key set to ""
                        } else {
                            md.put(mk, v);
                        }
                    }
                });
                return new Reply(200, inv);
            }
            case "POST /v1/invoices/create_preview" -> {
                if (!may("invoices:write")) {
                    return denied("rak_invoice_write");
                }
                String cus = b.getOrDefault("customer", "");
                if (!customers.containsKey(cus)) {
                    return error(400, "invalid_request_error", "resource_missing", "No such customer: '" + cus + "'", "customer");
                }
                ObjectNode prev = JSON.createObjectNode().put("object", "invoice").put("customer", cus).put("currency", b.getOrDefault("currency", "eur"))
                        .put("status", "draft");
                prev.putNull("id");
                ArrayNode lines = prev.putObject("lines").put("object", "list").putArray("data");
                long total = 0;
                for (int i = 0; b.containsKey("invoice_items[" + i + "][amount]"); i++) {
                    long a = Long.parseLong(b.get("invoice_items[" + i + "][amount]"));
                    total += a;
                    lines.addObject().put("amount", a).put("description", b.getOrDefault("invoice_items[" + i + "][description]", ""));
                }
                prev.put("subtotal", total).put("tax", 0).put("total", total).put("amount_due", total);
                return new Reply(200, prev);
            }
            default -> {
                return error(404, "invalid_request_error", null, "Unrecognized request URL (" + method + ": " + path + ").", null);
            }
        }
    }

    private Reply list(java.util.Collection<ObjectNode> all, Map<String, String> q) {
        int limit = Integer.parseInt(q.getOrDefault("limit", "10"));
        ObjectNode out = JSON.createObjectNode().put("object", "list").put("has_more", all.size() > limit);
        ArrayNode data = out.putArray("data");
        all.stream().limit(limit).forEach(data::add);
        return new Reply(200, out);
    }

    /** Events newest first, filtered by type and created[gt], paged with starting_after. */
    private Reply events(Map<String, String> q) {
        int limit = Math.min(100, Integer.parseInt(q.getOrDefault("limit", "10")));
        long gt = Long.parseLong(q.getOrDefault("created[gt]", "-1"));
        String type = q.get("type"), after = q.get("starting_after");
        List<ObjectNode> match = new ArrayList<>();
        synchronized (events) {
            for (int i = events.size() - 1; i >= 0; i--) {
                ObjectNode e = events.get(i);
                if ((type == null || e.path("type").asText().equals(type)) && e.path("created").asLong() > gt) {
                    match.add(e);
                }
            }
        }
        int from = 0;
        if (after != null) {
            for (int i = 0; i < match.size(); i++) {
                if (match.get(i).path("id").asText().equals(after)) {
                    from = i + 1;
                }
            }
        }
        ObjectNode out = JSON.createObjectNode().put("object", "list").put("has_more", match.size() > from + limit).put("url", "/v1/events");
        ArrayNode data = out.putArray("data");
        match.subList(from, Math.min(match.size(), from + limit)).forEach(data::add);
        return new Reply(200, out);
    }

    private Reply fake(String method, String path, Map<String, String> q, Map<String, String> b) {
        Matcher m = FAKE_INVOICE.matcher(path);
        if (method.equals("POST") && path.equals("/fake/customers")) {
            String id = addCustomer(b.getOrDefault("email", ""), b.getOrDefault("name", ""));
            return new Reply(200, customers.get(id));
        }
        if (method.equals("POST") && path.equals("/fake/pay")) {
            try {
                return new Reply(200, pay(q.getOrDefault("invoice", "")));
            } catch (IllegalStateException e) {
                return error(400, "invalid_request_error", null, e.getMessage(), "invoice");
            }
        }
        if (method.equals("GET") && m.matches()) {
            ObjectNode i = invoices.get(m.group(1));
            return i == null ? missing("invoice", m.group(1), "id") : new Reply(200, i);
        }
        if (method.equals("POST") && path.equals("/fake/throttle")) {
            throttle(Integer.parseInt(q.getOrDefault("n", "1")));
            return new Reply(200, JSON.createObjectNode().put("throttled", throttle.get()));
        }
        return error(404, "invalid_request_error", null, "Unrecognized request URL (" + method + ": " + path + ").", null);
    }

    private static void send(HttpExchange ex, Reply r) throws IOException {
        byte[] b = JSON.writeValueAsBytes(r.body);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.getResponseHeaders().add("Request-Id", "req_fake");
        ex.sendResponseHeaders(r.status, b.length);
        ex.getResponseBody().write(b);
    }

    /**
     * Runs the fake: {@code --port 9600 --key rk_test_demo [--permissions customers:read,invoices:read,invoices:write,events:read]
     * [--customer ada@example.com=Ada Lovelace GmbH]} (the customer flag repeats; their IDs are printed).
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> flags = new HashMap<>();
        List<String[]> seed = new ArrayList<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].equals("--customer")) {
                seed.add(args[i + 1].split("=", 2));
            } else {
                flags.put(args[i].replaceFirst("^--", ""), args[i + 1]);
            }
        }
        String perms = flags.get("permissions");
        FakeStripe f = new FakeStripe(Integer.parseInt(flags.getOrDefault("port", "9600")), flags.getOrDefault("key", "rk_test_demo"),
                perms == null ? null : Set.of(perms.split(",")));
        System.out.println("fake Stripe listening on " + f.url() + (perms == null ? " (secret key)" : " (restricted key: " + perms + ")"));
        for (String[] c : seed) {
            System.out.println("customer " + f.addCustomer(c[0], c.length > 1 ? c[1] : "") + " " + c[0]);
        }
        Thread.currentThread().join();
    }
}
