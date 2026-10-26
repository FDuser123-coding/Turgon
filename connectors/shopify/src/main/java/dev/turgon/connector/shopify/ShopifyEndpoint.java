package dev.turgon.connector.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.Event;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * One Shopify store through the GraphQL Admin API.
 *
 * <ul>
 * <li>Event {@code Order.Created}: new orders, polled in ID order
 * ({@code orders(sortKey: ID, query: "id:>N")}); the position is the
 * order's numeric ID.
 * <li>Reads {@code get-order} and {@code get-customer}, by numeric or GID.
 * <li>Write {@code update-order}: sets an order's note and tags
 * ({@code orderUpdate}); its result keeps the values it replaced, which its
 * compensation {@code restore-order} puts back.
 * </ul>
 *
 * Orders and customers are given in the shape of Shopify's webhooks and
 * REST resources ({@code admin_graphql_api_id}, {@code created_at},
 * {@code subtotal_price}, {@code line_items}), so mappings written for an
 * {@code orders/create} webhook read them unchanged.
 */
final class ShopifyEndpoint implements Endpoint {
    static final String EVENT = "Order.Created";
    static final Set<String> SCOPES = Set.of("read_orders", "write_orders", "read_customers");

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String MONEY = "shopMoney { amount currencyCode }";
    private static final String CUSTOMER = "id legacyResourceId firstName lastName defaultEmailAddress { emailAddress }";
    private static final String ORDER = "id legacyResourceId name createdAt updatedAt email currencyCode note tags displayFinancialStatus"
            + " subtotalPriceSet { " + MONEY + " } totalPriceSet { " + MONEY + " } totalTaxSet { " + MONEY + " }"
            + " customer { " + CUSTOMER + " }"
            + " shippingLines(first: 10) { nodes { code title originalPriceSet { " + MONEY + " } } }"
            + " lineItems(first: 250) { nodes { id sku name quantity originalUnitPriceSet { " + MONEY + " } } }";
    static final String Q_ORDERS = "query Orders($first: Int!, $query: String!) { orders(first: $first, sortKey: ID, query: $query) { nodes { " + ORDER + " } } }";
    static final String Q_ORDER = "query Order($id: ID!) { order(id: $id) { " + ORDER + " } }";
    static final String Q_CUSTOMER = "query Customer($id: ID!) { customer(id: $id) { " + CUSTOMER + " } }";
    static final String M_UPDATE = "mutation OrderUpdate($input: OrderInput!) { orderUpdate(input: $input) { order { id note tags } userErrors { field message } } }";
    static final String Q_SHOP = "query Shop { shop { name myshopifyDomain currencyCode } }";
    static final String Q_SCOPES = "query Scopes { currentAppInstallation { accessScopes { handle } } }";
    static final String Q_TYPE = "query Type($name: String!) { __type(name: $name) { name kind fields { name type { kind name ofType { kind name } } } } }";

    /** The Order and Customer fields the connector reads, for the metadata graph. */
    private static final List<String> ORDER_FIELDS = List.of("id", "legacyResourceId", "name", "createdAt", "updatedAt", "email", "currencyCode",
            "note", "tags", "displayFinancialStatus", "subtotalPriceSet", "totalPriceSet", "totalTaxSet", "customer", "shippingLines", "lineItems");
    private static final List<String> CUSTOMER_FIELDS = List.of("id", "legacyResourceId", "firstName", "lastName", "defaultEmailAddress");

    private final String name;
    private final ShopifyConfig cfg;
    private final ShopifyClient client;

    ShopifyEndpoint(String name, ShopifyConfig cfg, ShopifyClient client) {
        this.name = name;
        this.cfg = cfg;
        this.client = client;
    }

    /** "5600000000001" or "gid://shopify/Order/5600000000001" as a GID. */
    static String gid(String type, String id) throws ConnectorException.Invalid {
        String s = id == null ? "" : id.trim();
        if (s.startsWith("gid://shopify/" + type + "/")) {
            return s;
        }
        if (!s.matches("\\d{1,20}")) {
            throw new ConnectorException.Invalid("a " + type + " ID is a number or gid://shopify/" + type + "/<number>");
        }
        return "gid://shopify/" + type + "/" + s;
    }

    private static ObjectNode vars() {
        return JSON.createObjectNode();
    }

    @Override
    public List<Event> poll(String event, long after, int limit) throws Exception {
        if (!event.equals(EVENT)) {
            throw new ConnectorException.Invalid("the connector emits " + EVENT + ", not " + event);
        }
        JsonNode data = client.run("Orders", Q_ORDERS, vars().put("first", Math.max(1, Math.min(limit, cfg.pageSize())))
                .put("query", "id:>" + Math.max(0, after)));
        List<Event> out = new ArrayList<>();
        for (JsonNode o : data.path("orders").path("nodes")) {
            ObjectNode rest = order(o);
            out.add(new Event(rest.get("admin_graphql_api_id").asText(), rest.get("id").asLong(), EVENT, rest));
        }
        return out;
    }

    @Override
    public Set<String> streams() {
        return Set.of();
    }

    @Override
    public JsonNode read(String operation, String id) throws Exception {
        switch (operation) {
            case "get-order" -> {
                JsonNode o = client.run("Order", Q_ORDER, vars().put("id", gid("Order", id))).path("order");
                if (!o.isObject()) {
                    throw new ConnectorException.NotFound("no order " + id);
                }
                return order(o);
            }
            case "get-customer" -> {
                JsonNode c = client.run("Customer", Q_CUSTOMER, vars().put("id", gid("Customer", id))).path("customer");
                if (!c.isObject()) {
                    throw new ConnectorException.NotFound("no customer " + id);
                }
                return customer(c);
            }
            default -> throw new ConnectorException.Invalid("the connector has no read operation " + operation);
        }
    }

    /** What update-order changes: {"orderId", "note"?, "tags"?}. */
    private record Change(String gid, JsonNode note, JsonNode tags) {}

    private static Change change(JsonNode p, boolean restoring) throws ConnectorException.Invalid {
        if (p == null || !p.isObject()) {
            throw new ConnectorException.Invalid("the payload must be an object");
        }
        JsonNode src = restoring ? p.path("previous") : p;
        String id = p.path("orderId").asText(p.path("admin_graphql_api_id").asText(p.path("id").asText("")));
        if (restoring && !src.isObject()) {
            throw new ConnectorException.Invalid("restore-order takes update-order's result, with its previous values");
        }
        JsonNode note = src.get("note"), tags = src.get("tags");
        if (note != null && !(note.isTextual() || note.isNull())) {
            throw new ConnectorException.Invalid("note must be text");
        }
        if (tags != null && !(tags.isArray() || tags.isTextual())) {
            throw new ConnectorException.Invalid("tags must be a list of text, or text separated by commas");
        }
        if (tags != null && tags.isTextual()) {
            ArrayNode a = JSON.createArrayNode();
            for (String t : tags.asText().split(",")) {
                if (!t.isBlank()) {
                    a.add(t.trim());
                }
            }
            tags = a;
        }
        if (note == null && tags == null) {
            throw new ConnectorException.Invalid("the payload changes nothing: set note or tags");
        }
        return new Change(gid("Order", id), note, tags);
    }

    private ObjectNode current(String gid) throws Exception {
        JsonNode o = client.run("Order", Q_ORDER, vars().put("id", gid)).path("order");
        if (!o.isObject()) {
            throw new ConnectorException.NotFound("no order " + gid);
        }
        return (ObjectNode) o;
    }

    @Override
    public JsonNode simulate(String operation, JsonNode payload) throws Exception {
        Change c = change(payload, operation.equals("restore-order"));
        requireWrite(operation);
        ObjectNode cur = current(c.gid());
        ObjectNode out = JSON.createObjectNode().put("orderId", c.gid()).put("order", cur.path("name").asText());
        ObjectNode was = out.putObject("current"), will = out.putObject("proposed");
        if (c.note() != null) {
            was.set("note", cur.get("note"));
            will.set("note", c.note());
        }
        if (c.tags() != null) {
            was.set("tags", cur.get("tags"));
            will.set("tags", c.tags());
        }
        return out;
    }

    private static void requireWrite(String operation) throws ConnectorException.Invalid {
        if (!operation.equals("update-order") && !operation.equals("restore-order")) {
            throw new ConnectorException.Invalid("the connector has no write operation " + operation);
        }
    }

    /**
     * Sets the note and tags. Setting a value twice leaves it as once, so a
     * retried update needs no key; its result records what it replaced.
     */
    @Override
    public JsonNode commit(String operation, String key, JsonNode payload) throws Exception {
        requireWrite(operation);
        boolean restoring = operation.equals("restore-order");
        Change c = change(payload, restoring);
        ObjectNode before = current(c.gid());
        ObjectNode input = JSON.createObjectNode().put("id", c.gid());
        if (c.note() != null) {
            input.set("note", c.note());
        }
        if (c.tags() != null) {
            input.set("tags", c.tags());
        }
        JsonNode res = client.run("OrderUpdate", M_UPDATE, (ObjectNode) vars().set("input", input)).path("orderUpdate");
        JsonNode errs = res.path("userErrors");
        if (errs.isArray() && !errs.isEmpty()) {
            List<String> msgs = new ArrayList<>();
            errs.forEach(e -> msgs.add(e.path("message").asText()));
            throw new ConnectorException.Rejected("Shopify refused the order update: " + String.join("; ", msgs));
        }
        ObjectNode out = JSON.createObjectNode().put("orderId", c.gid()).put("order", before.path("name").asText());
        if (c.note() != null) {
            out.set("note", res.path("order").path("note"));
        }
        if (c.tags() != null) {
            out.set("tags", res.path("order").path("tags"));
        }
        if (!restoring) {
            ObjectNode prev = out.putObject("previous");
            if (c.note() != null) {
                prev.set("note", before.get("note"));
            }
            if (c.tags() != null) {
                prev.set("tags", before.get("tags"));
            }
        }
        return out;
    }

    @Override
    public void confirm(String operation, JsonNode result) throws Exception {
        requireWrite(operation);
        ObjectNode cur = current(gid("Order", result.path("orderId").asText()));
        for (String f : List.of("note", "tags")) {
            if (result.has(f) && !result.get(f).equals(cur.get(f))) {
                throw new ConnectorException.Rejected("the order's " + f + " is not what was written: something changed it since");
            }
        }
    }

    @Override
    public List<CheckResult> check() {
        List<CheckResult> out = new ArrayList<>();
        try {
            JsonNode shop = client.run("Shop", Q_SHOP, null).path("shop");
            String domain = shop.path("myshopifyDomain").asText();
            if (!domain.equalsIgnoreCase(cfg.shop())) {
                out.add(CheckResult.fail("store", "the token belongs to " + domain + ", not " + cfg.shop(),
                        "Use the access token of the app installed on " + cfg.shop() + ", or correct config.shop."));
                return out;
            }
            out.add(CheckResult.pass("store", shop.path("name").asText() + " (" + domain + ", " + shop.path("currencyCode").asText() + ")"));
        } catch (ShopifyClient.Unauthorized e) {
            out.add(CheckResult.fail("store", e.getMessage(), "The access token is wrong or the app was uninstalled. Reinstall the app and store its Admin API access token (shpat_...) in the secret."));
            return out;
        } catch (Exception e) {
            out.add(CheckResult.fail("store", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "Shopify did not answer at " + cfg.endpoint().getHost() + ". Allow outbound HTTPS to it from this node."));
            return out;
        }
        String served = client.served();
        if (!served.isEmpty() && !served.equals(cfg.apiVersion())) {
            out.add(CheckResult.fail("api version", "asked for " + cfg.apiVersion() + ", Shopify answered with " + served,
                    "Shopify no longer supports " + cfg.apiVersion() + " and serves the oldest supported version instead. Test the connection against "
                            + served + " and set config.apiVersion to it."));
        } else {
            out.add(CheckResult.pass("api version", cfg.apiVersion()));
        }
        try {
            Set<String> granted = new LinkedHashSet<>();
            client.run("Scopes", Q_SCOPES, null).path("currentAppInstallation").path("accessScopes").forEach(s -> granted.add(s.path("handle").asText()));
            List<String> missing = new ArrayList<>();
            for (String s : new java.util.TreeSet<>(SCOPES)) {
                // A write scope implies its read scope.
                boolean implied = s.startsWith("read_") && granted.contains("write_" + s.substring(5));
                if (!granted.contains(s) && !implied) {
                    missing.add(s);
                }
            }
            if (missing.isEmpty()) {
                out.add(CheckResult.pass("scopes", String.join(", ", granted)));
            } else {
                out.add(CheckResult.fail("scopes", "missing " + String.join(", ", missing),
                        "Add " + String.join(", ", missing) + " to the app's access scopes and reinstall it: orders are read, noted and tagged, and their customers read."));
            }
        } catch (Exception e) {
            out.add(CheckResult.fail("scopes", e.getMessage(), ""));
        }
        return out;
    }

    /**
     * Order and Customer as the store's schema describes them (GraphQL
     * introspection), and the fields the connector reads from them. More
     * types can be named.
     */
    @Override
    public JsonNode discover(List<String> objects) throws Exception {
        ObjectNode cat = JSON.createObjectNode();
        ArrayNode objs = cat.putArray("objects"), uses = cat.putArray("uses");
        Set<String> types = new LinkedHashSet<>(List.of("Order", "Customer"));
        types.addAll(objects);
        for (String t : types) {
            JsonNode type = client.run("Type", Q_TYPE, vars().put("name", t)).path("__type");
            if (!type.isObject()) {
                continue; // not in this API version: drift shows what uses it
            }
            ObjectNode o = objs.addObject().put("name", t).put("kind", "graphql-object");
            ArrayNode fields = o.putArray("fields");
            for (JsonNode f : type.path("fields")) {
                JsonNode ft = f.path("type");
                boolean nonNull = ft.path("kind").asText().equals("NON_NULL");
                String tn = nonNull ? ft.path("ofType").path("name").asText() : ft.path("name").asText();
                fields.addObject().put("name", f.path("name").asText()).put("type", tn).put("required", nonNull);
            }
        }
        for (String f : ORDER_FIELDS) {
            uses.addObject().put("object", "Order").put("field", f).put("by", "event " + EVENT);
        }
        uses.addObject().put("object", "Order").put("field", "note").put("by", "operation update-order");
        uses.addObject().put("object", "Order").put("field", "tags").put("by", "operation update-order");
        for (String f : CUSTOMER_FIELDS) {
            uses.addObject().put("object", "Customer").put("field", f).put("by", "operation get-customer");
        }
        return cat;
    }

    private static String amount(JsonNode set) {
        return set.path("shopMoney").path("amount").asText("0.00");
    }

    /** A GraphQL Customer in the shape of the REST resource. */
    static ObjectNode customer(JsonNode c) {
        ObjectNode r = JSON.createObjectNode();
        r.put("id", c.path("legacyResourceId").asLong()).put("admin_graphql_api_id", c.path("id").asText())
                .put("email", c.path("defaultEmailAddress").path("emailAddress").asText(null))
                .put("first_name", c.path("firstName").asText(null)).put("last_name", c.path("lastName").asText(null));
        return r;
    }

    /** A GraphQL Order in the shape of the orders/create webhook. */
    static ObjectNode order(JsonNode o) {
        ObjectNode r = JSON.createObjectNode();
        String name = o.path("name").asText("");
        r.put("id", o.path("legacyResourceId").asLong()).put("admin_graphql_api_id", o.path("id").asText()).put("name", name);
        String digits = name.replaceAll("\\D", "");
        if (!digits.isEmpty()) {
            r.put("order_number", Long.parseLong(digits));
        }
        r.put("created_at", o.path("createdAt").asText(null)).put("updated_at", o.path("updatedAt").asText(null))
                .put("email", o.path("email").asText(null)).put("currency", o.path("currencyCode").asText(null))
                .put("subtotal_price", amount(o.path("subtotalPriceSet"))).put("total_price", amount(o.path("totalPriceSet")))
                .put("total_tax", amount(o.path("totalTaxSet"))).put("financial_status", o.path("displayFinancialStatus").asText("").toLowerCase(java.util.Locale.ROOT));
        r.set("note", o.path("note").isMissingNode() ? JSON.nullNode() : o.get("note"));
        List<String> tags = new ArrayList<>();
        o.path("tags").forEach(t -> tags.add(t.asText()));
        r.put("tags", String.join(", ", tags));
        JsonNode c = o.path("customer");
        if (c.isObject()) {
            r.set("customer", customer(c));
        } else {
            r.putNull("customer");
        }
        ArrayNode ship = r.putArray("shipping_lines");
        for (JsonNode s : o.path("shippingLines").path("nodes")) {
            ship.addObject().put("code", s.path("code").asText(null)).put("title", s.path("title").asText(null)).put("price", amount(s.path("originalPriceSet")));
        }
        ArrayNode lines = r.putArray("line_items");
        for (JsonNode l : o.path("lineItems").path("nodes")) {
            String lid = l.path("id").asText("");
            ObjectNode li = lines.addObject();
            String num = lid.substring(lid.lastIndexOf('/') + 1);
            if (num.matches("\\d+")) {
                li.put("id", Long.parseLong(num));
            }
            li.put("admin_graphql_api_id", lid).put("sku", l.path("sku").asText(null)).put("name", l.path("name").asText(null))
                    .put("quantity", l.path("quantity").asInt()).put("price", amount(l.path("originalUnitPriceSet")));
        }
        return r;
    }

    @Override
    public String toString() {
        return name + " " + cfg.shop();
    }
}
