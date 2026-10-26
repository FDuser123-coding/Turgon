package dev.turgon.connector.stripe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.Event;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * One Stripe account.
 *
 * <ul>
 * <li>Event {@code Invoice.Paid}: Stripe's {@code invoice.paid} events, as
 * Stripe sends them to a webhook ({@code data.object} is the invoice). The
 * position is the event's creation time (Unix seconds). Stripe lists events
 * newest first, so a poll pages back to the cursor and hands them over oldest
 * first.
 * <li>Write {@code create-invoice}: an invoice for a customer with one item
 * per line, finalized unless configured otherwise. Amounts are in the
 * currency's major unit and converted to Stripe's minor unit. Every request
 * carries an idempotency key derived from the write's, so a retried
 * create returns the same invoice. The dry run is Stripe's invoice preview,
 * with its totals. {@code void-invoice}, its compensation, deletes a draft
 * and voids an open invoice.
 * <li>Write {@code link-invoice}: sets a metadata key on an invoice (the ERP
 * payment it was recorded as); {@code unlink-invoice} puts the previous
 * value back.
 * <li>Reads {@code get-invoice} and {@code get-customer}.
 * </ul>
 */
final class StripeEndpoint implements Endpoint {
    static final String EVENT = "Invoice.Paid";

    private static final ObjectMapper JSON = new ObjectMapper();
    /** Currencies without minor units, and with three decimals (Stripe's lists). */
    private static final Set<String> ZERO_DECIMAL = Set.of("bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf", "ugx", "vnd",
            "vuv", "xaf", "xof", "xpf");
    private static final Set<String> THREE_DECIMAL = Set.of("bhd", "jod", "kwd", "omr", "tnd");
    /** How far back a poll pages at most: 20 pages of 100 events. */
    private static final int MAX_PAGES = 20;

    private final String name;
    private final StripeConfig cfg;
    private final StripeClient client;

    StripeEndpoint(String name, StripeConfig cfg, StripeClient client) {
        this.name = name;
        this.cfg = cfg;
        this.client = client;
    }

    static int exponent(String currency) {
        String c = currency.toLowerCase(Locale.ROOT);
        return ZERO_DECIMAL.contains(c) ? 0 : THREE_DECIMAL.contains(c) ? 3 : 2;
    }

    /** A major-unit amount in the currency's minor unit. */
    static long minor(BigDecimal amount, String currency) throws ConnectorException.Invalid {
        BigDecimal m = amount.movePointRight(exponent(currency));
        if (m.stripTrailingZeros().scale() > 0) {
            throw new ConnectorException.Invalid("an amount has more decimals than " + currency.toUpperCase(Locale.ROOT) + " allows");
        }
        return m.longValueExact();
    }

    static BigDecimal major(long minor, String currency) {
        return BigDecimal.valueOf(minor).movePointLeft(exponent(currency));
    }

    private static String id(JsonNode p, String field, String prefix) throws ConnectorException.Invalid {
        String v = p.path(field).asText("").trim();
        if (!v.startsWith(prefix) || v.length() < prefix.length() + 1) {
            throw new ConnectorException.Invalid(field + " must be a Stripe ID (" + prefix + "...)");
        }
        return v;
    }

    @Override
    public List<Event> poll(String event, long after, int limit) throws Exception {
        if (!event.equals(EVENT)) {
            throw new ConnectorException.Invalid("the connector emits " + EVENT + ", not " + event);
        }
        List<JsonNode> newestFirst = new ArrayList<>();
        String startingAfter = null;
        for (int page = 0; page < MAX_PAGES; page++) {
            Map<String, String> q = new LinkedHashMap<>();
            q.put("type", "invoice.paid");
            q.put("created[gt]", Long.toString(Math.max(0, after)));
            q.put("limit", Integer.toString(cfg.pageSize()));
            if (startingAfter != null) {
                q.put("starting_after", startingAfter);
            }
            JsonNode list = client.get("/v1/events", q);
            for (JsonNode e : list.path("data")) {
                newestFirst.add(e);
                startingAfter = e.path("id").asText();
            }
            if (!list.path("has_more").asBoolean(false) || list.path("data").isEmpty()) {
                break;
            }
        }
        Collections.reverse(newestFirst);
        List<Event> out = new ArrayList<>();
        for (JsonNode e : newestFirst) {
            if (out.size() >= limit) {
                break;
            }
            out.add(new Event(e.path("id").asText(), e.path("created").asLong(), EVENT, e));
        }
        return out;
    }

    @Override
    public JsonNode read(String operation, String id) throws Exception {
        return switch (operation) {
            case "get-invoice" -> client.get("/v1/invoices/" + id(JSON.createObjectNode().put("id", id), "id", "in_"), Map.of());
            case "get-customer" -> client.get("/v1/customers/" + id(JSON.createObjectNode().put("id", id), "id", "cus_"), Map.of());
            default -> throw new ConnectorException.Invalid("the connector has no read operation " + operation);
        };
    }

    /** create-invoice's payload: {"customerId", "currency", "lines": [{"description", "amount", "quantity"}], "memo"?, "metadata"?}. */
    private record Draft(String customer, String currency, List<String> descriptions, List<Long> amounts, String memo, Map<String, String> metadata) {
        long total() {
            return amounts.stream().mapToLong(Long::longValue).sum();
        }
    }

    private static Draft draft(JsonNode p) throws ConnectorException.Invalid {
        if (p == null || !p.isObject()) {
            throw new ConnectorException.Invalid("the payload must be an object");
        }
        String customer = id(p, "customerId", "cus_");
        String currency = p.path("currency").asText("").trim().toLowerCase(Locale.ROOT);
        if (!currency.matches("[a-z]{3}")) {
            throw new ConnectorException.Invalid("currency must be a three-letter ISO code");
        }
        JsonNode lines = p.path("lines");
        if (!lines.isArray() || lines.isEmpty()) {
            throw new ConnectorException.Invalid("lines must list at least one line");
        }
        List<String> desc = new ArrayList<>();
        List<Long> amounts = new ArrayList<>();
        int i = 0;
        for (JsonNode l : lines) {
            i++;
            BigDecimal unit;
            try {
                unit = new BigDecimal(l.path("amount").asText(""));
            } catch (NumberFormatException x) {
                throw new ConnectorException.Invalid("line " + i + ": amount must be a number");
            }
            long qty = l.path("quantity").asLong(1);
            if (qty < 1) {
                throw new ConnectorException.Invalid("line " + i + ": quantity must be at least 1");
            }
            long amount = minor(unit.multiply(BigDecimal.valueOf(qty)), currency);
            String d = l.path("description").asText("").trim();
            if (d.isEmpty()) {
                throw new ConnectorException.Invalid("line " + i + ": description is required");
            }
            desc.add(qty > 1 ? d + " × " + qty : d);
            amounts.add(amount);
        }
        Map<String, String> md = new LinkedHashMap<>();
        for (var f : p.path("metadata").properties()) {
            md.put(f.getKey(), f.getValue().asText());
        }
        return new Draft(customer, currency, desc, amounts, p.path("memo").asText(""), md);
    }

    @Override
    public JsonNode simulate(String operation, JsonNode payload) throws Exception {
        switch (operation) {
            case "create-invoice" -> {
                Draft d = draft(payload);
                Map<String, String> params = new LinkedHashMap<>();
                params.put("customer", d.customer());
                params.put("currency", d.currency());
                for (int i = 0; i < d.amounts().size(); i++) {
                    params.put("invoice_items[" + i + "][amount]", Long.toString(d.amounts().get(i)));
                    params.put("invoice_items[" + i + "][currency]", d.currency());
                    params.put("invoice_items[" + i + "][description]", d.descriptions().get(i));
                }
                // Stripe computes the invoice, taxes included, without creating it.
                JsonNode prev = client.post("/v1/invoices/create_preview", params, null);
                ObjectNode out = JSON.createObjectNode().put("customer", d.customer()).put("currency", d.currency())
                        .put("subtotal", major(prev.path("subtotal").asLong(), d.currency())).put("tax", major(prev.path("tax").asLong(), d.currency()))
                        .put("total", major(prev.path("total").asLong(), d.currency())).put("finalize", cfg.finalizeInvoices()).put("simulated", true);
                ArrayNode lines = out.putArray("lines");
                for (JsonNode l : prev.path("lines").path("data")) {
                    lines.addObject().put("description", l.path("description").asText()).put("amount", major(l.path("amount").asLong(), d.currency()));
                }
                return out;
            }
            case "link-invoice" -> {
                String inv = id(payload, "invoiceId", "in_");
                JsonNode cur = client.get("/v1/invoices/" + inv, Map.of());
                ObjectNode out = JSON.createObjectNode().put("invoiceId", inv);
                out.putObject("current").set(cfg.metadataKey(), cur.path("metadata").path(cfg.metadataKey()).isMissingNode() ? JSON.nullNode()
                        : cur.path("metadata").get(cfg.metadataKey()));
                out.putObject("proposed").put(cfg.metadataKey(), linkValue(payload));
                return out;
            }
            case "void-invoice", "unlink-invoice" -> {
                return JSON.createObjectNode().put("operation", operation).set("of", payload);
            }
            default -> throw new ConnectorException.Invalid("the connector has no write operation " + operation);
        }
    }

    private static String linkValue(JsonNode p) throws ConnectorException.Invalid {
        String v = p.path("erpPaymentId").asText("").trim();
        if (v.isEmpty()) {
            throw new ConnectorException.Invalid("erpPaymentId is required");
        }
        return v;
    }

    @Override
    public JsonNode commit(String operation, String key, JsonNode payload) throws Exception {
        return switch (operation) {
            case "create-invoice" -> create(key, draft(payload));
            case "void-invoice" -> voidInvoice(payload);
            case "link-invoice" -> link(key, payload);
            case "unlink-invoice" -> unlink(key, payload);
            default -> throw new ConnectorException.Invalid("the connector has no write operation " + operation);
        };
    }

    private ObjectNode create(String key, Draft d) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("customer", d.customer());
        params.put("currency", d.currency());
        params.put("collection_method", cfg.collectionMethod());
        if (cfg.collectionMethod().equals("send_invoice")) {
            params.put("days_until_due", Integer.toString(cfg.daysUntilDue()));
        }
        params.put("auto_advance", "false");
        params.put("pending_invoice_items_behavior", "exclude");
        if (!d.memo().isEmpty()) {
            params.put("description", d.memo());
        }
        d.metadata().forEach((k, v) -> params.put("metadata[" + k + "]", v));
        params.put("metadata[turgon_key]", key);
        JsonNode inv = client.post("/v1/invoices", params, key + "/invoice");
        String id = inv.path("id").asText();
        for (int i = 0; i < d.amounts().size(); i++) {
            Map<String, String> item = new LinkedHashMap<>();
            item.put("customer", d.customer());
            item.put("invoice", id);
            item.put("currency", d.currency());
            item.put("amount", Long.toString(d.amounts().get(i)));
            item.put("description", d.descriptions().get(i));
            client.post("/v1/invoiceitems", item, key + "/item/" + i);
        }
        if (cfg.finalizeInvoices()) {
            inv = client.post("/v1/invoices/" + id + "/finalize", Map.of(), key + "/finalize");
        } else {
            inv = client.get("/v1/invoices/" + id, Map.of());
        }
        return summary(inv);
    }

    private static ObjectNode summary(JsonNode inv) {
        String cur = inv.path("currency").asText("eur");
        ObjectNode out = JSON.createObjectNode().put("id", inv.path("id").asText()).put("status", inv.path("status").asText())
                .put("customer", inv.path("customer").asText()).put("currency", cur).put("total", major(inv.path("total").asLong(), cur));
        out.set("number", inv.path("number").isMissingNode() ? JSON.nullNode() : inv.get("number"));
        return out;
    }

    /** Deletes a draft, voids an open invoice; one already voided or deleted is left as it is. */
    private ObjectNode voidInvoice(JsonNode p) throws Exception {
        String id = id(p, "id", "in_");
        JsonNode inv;
        try {
            inv = client.get("/v1/invoices/" + id, Map.of());
        } catch (ConnectorException.NotFound gone) {
            return JSON.createObjectNode().put("id", id).put("status", "deleted");
        }
        return switch (inv.path("status").asText()) {
            case "draft" -> {
                client.delete("/v1/invoices/" + id);
                yield JSON.createObjectNode().put("id", id).put("status", "deleted");
            }
            case "open" -> summary(client.post("/v1/invoices/" + id + "/void", Map.of(), "void/" + id));
            case "void" -> summary(inv);
            case "paid" -> throw new ConnectorException.Rejected("invoice " + id + " is paid and cannot be voided: refund its payment instead");
            default -> throw new ConnectorException.Rejected("invoice " + id + " is " + inv.path("status").asText() + " and cannot be voided");
        };
    }

    private ObjectNode link(String key, JsonNode p) throws Exception {
        String id = id(p, "invoiceId", "in_");
        String value = linkValue(p);
        JsonNode before = client.get("/v1/invoices/" + id, Map.of()).path("metadata").path(cfg.metadataKey());
        Map<String, String> params = Map.of("metadata[" + cfg.metadataKey() + "]", value);
        JsonNode inv = client.post("/v1/invoices/" + id, params, key);
        ObjectNode out = JSON.createObjectNode().put("invoiceId", id);
        out.putObject("metadata").set(cfg.metadataKey(), inv.path("metadata").path(cfg.metadataKey()));
        out.putObject("previous").set(cfg.metadataKey(), before.isMissingNode() ? JSON.nullNode() : before);
        return out;
    }

    private ObjectNode unlink(String key, JsonNode p) throws Exception {
        String id = id(p, "invoiceId", "in_");
        JsonNode prev = p.path("previous");
        if (!prev.isObject() || !prev.has(cfg.metadataKey())) {
            throw new ConnectorException.Invalid("unlink-invoice takes link-invoice's result, with its previous value");
        }
        JsonNode old = prev.get(cfg.metadataKey());
        // Stripe deletes a metadata key set to "".
        Map<String, String> params = Map.of("metadata[" + cfg.metadataKey() + "]", old.isNull() ? "" : old.asText());
        JsonNode inv = client.post("/v1/invoices/" + id, params, key);
        ObjectNode out = JSON.createObjectNode().put("invoiceId", id);
        out.putObject("metadata").set(cfg.metadataKey(), inv.path("metadata").path(cfg.metadataKey()).isMissingNode() ? JSON.nullNode()
                : inv.path("metadata").get(cfg.metadataKey()));
        return out;
    }

    @Override
    public void confirm(String operation, JsonNode result) throws Exception {
        switch (operation) {
            case "create-invoice" -> {
                JsonNode inv = client.get("/v1/invoices/" + id(result, "id", "in_"), Map.of());
                if (!inv.path("status").asText().equals(result.path("status").asText())) {
                    throw new ConnectorException.Rejected("invoice " + result.path("id").asText() + " is " + inv.path("status").asText() + ", not "
                            + result.path("status").asText());
                }
            }
            case "link-invoice" -> {
                JsonNode inv = client.get("/v1/invoices/" + id(result, "invoiceId", "in_"), Map.of());
                if (!inv.path("metadata").path(cfg.metadataKey()).equals(result.path("metadata").path(cfg.metadataKey()))) {
                    throw new ConnectorException.Rejected("the invoice's " + cfg.metadataKey() + " is not what was written");
                }
            }
            default -> {
                // Compensations are read back by the next run's dry run, not here.
            }
        }
    }

    /**
     * The key's mode and what it may read. Writes cannot be probed without
     * writing: the first create or link reports a missing permission.
     */
    @Override
    public List<CheckResult> check() {
        List<CheckResult> out = new ArrayList<>();
        String mode = client.mode();
        if (mode.isEmpty()) {
            out.add(CheckResult.fail("key", "not a Stripe secret or restricted key",
                    "Store a restricted key (rk_live_... or rk_test_...) in the secret: create it in the Stripe Dashboard under Developers, API keys."));
            return out;
        }
        out.add(CheckResult.pass("key", mode + " mode" + (mode.equals("test") ? ": test-mode invoices are not real" : "")));
        for (String[] probe : List.of(new String[] {"customers", "/v1/customers", "Customers: Read"}, new String[] {"invoices", "/v1/invoices", "Invoices: Write"},
                new String[] {"events", "/v1/events", "Events: Read"})) {
            try {
                client.get(probe[1], Map.of("limit", "1"));
                out.add(CheckResult.pass(probe[0], "readable"));
            } catch (StripeClient.Unauthorized e) {
                out.add(CheckResult.fail(probe[0], e.getMessage(), "The key is wrong or was rolled. Store the current key in the secret."));
                return out;
            } catch (ConnectorException.Rejected e) {
                out.add(CheckResult.fail(probe[0], e.getMessage(), "Give the restricted key the permission " + probe[2] + "."));
            } catch (Exception e) {
                out.add(CheckResult.fail(probe[0], e.getClass().getSimpleName() + ": " + e.getMessage(),
                        "Stripe did not answer at " + cfg.url().getHost() + ". Allow outbound HTTPS to it from this node."));
                return out;
            }
        }
        return out;
    }

    @Override
    public String toString() {
        return name + " " + cfg.url().getHost();
    }
}
