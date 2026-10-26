package dev.turgon.connector.shopify;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import dev.turgon.connector.sdk.Event;
import dev.turgon.connector.shopify.fake.FakeShopify;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShopifyEndpointTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> ALL = Set.of("read_orders", "write_orders", "read_customers");

    private FakeShopify shop;

    @BeforeEach
    void start() throws Exception {
        shop = fake(ALL);
    }

    @AfterEach
    void stop() {
        shop.close();
    }

    private static FakeShopify fake(Set<String> scopes) throws IOException {
        return new FakeShopify(0, "turgon-demo.myshopify.com", "shpat_demo", Set.of("2026-07", "2026-10"), "2026-10", scopes);
    }

    private static Endpoint bind(FakeShopify s, String version, String token) throws Exception {
        JsonNode cfg = JSON.createObjectNode().put("shop", "turgon-demo.myshopify.com").put("apiVersion", version).put("url", s.url());
        return new ShopifyConnector().bind(new EndpointConfig("shopify-admin", "shopify", "2.1.0", cfg, token, Map.of()));
    }

    private Endpoint bind() throws Exception {
        return bind(shop, "2026-07", "shpat_demo");
    }

    private static JsonNode json(String s) throws IOException {
        return JSON.readTree(s);
    }

    private long order(String email, String subtotal) throws IOException {
        return shop.addOrder(json("""
                {"email": "%s", "currency": "EUR", "subtotal": "%s", "created_at": "2026-10-19T09:30:00Z", "shipping_code": "express",
                 "customer": {"email": "%s", "first_name": "Ada", "last_name": "Lovelace"},
                 "line_items": [{"sku": "M-7", "name": "Widget", "quantity": 2, "price": "174.95"}]}""".formatted(email, subtotal, email)));
    }

    @Test
    void checksPassForTheRightStoreTokenVersionAndScopes() throws Exception {
        try (Endpoint e = bind()) {
            List<CheckResult> checks = e.check();
            for (CheckResult c : checks) {
                assertTrue(c.ok(), c.toString());
            }
            assertEquals(List.of("store", "api version", "scopes"), checks.stream().map(CheckResult::name).toList());
            assertTrue(checks.get(0).detail().contains("turgon-demo.myshopify.com"), checks.get(0).detail());
        }
    }

    @Test
    void checksExplainAWrongTokenAnOldVersionAndMissingScopes() throws Exception {
        try (Endpoint e = bind(shop, "2026-07", "shpat_wrong")) {
            CheckResult c = e.check().get(0);
            assertFalse(c.ok());
            assertTrue(c.fix().contains("Reinstall the app"), c.fix());
        }
        try (Endpoint e = bind(shop, "2025-04", "shpat_demo")) {
            CheckResult v = e.check().get(1);
            assertFalse(v.ok());
            assertTrue(v.detail().contains("answered with 2026-10") && v.fix().contains("config.apiVersion"), v.toString());
        }
        try (FakeShopify narrow = fake(Set.of("read_orders")); Endpoint e = bind(narrow, "2026-07", "shpat_demo")) {
            CheckResult s = e.check().get(2);
            assertFalse(s.ok());
            assertTrue(s.detail().contains("read_customers, write_orders"), s.detail());
        }
    }

    @Test
    void newOrdersArePolledInIdOrderInTheWebhookShape() throws Exception {
        long a = order("ada@example.com", "349.90"), b = order("grace@example.com", "80.00"), c = order("ada@example.com", "12.00");
        try (Endpoint e = bind()) {
            List<Event> first = e.poll("Order.Created", 0, 2);
            assertEquals(List.of(a, b), first.stream().map(Event::position).toList());
            JsonNode p = first.get(0).payload();
            assertEquals("gid://shopify/Order/" + a, first.get(0).id());
            assertEquals("gid://shopify/Order/" + a, p.path("admin_graphql_api_id").asText());
            assertEquals(1001, p.path("order_number").asInt());
            assertEquals("ada@example.com", p.path("customer").path("email").asText());
            assertEquals("349.90", p.path("subtotal_price").asText());
            assertEquals("EUR", p.path("currency").asText());
            assertEquals("2026-10-19T09:30:00Z", p.path("created_at").asText());
            assertEquals("express", p.path("shipping_lines").get(0).path("code").asText());
            assertEquals("M-7", p.path("line_items").get(0).path("sku").asText());
            assertEquals(2, p.path("line_items").get(0).path("quantity").asInt());
            assertEquals(List.of(c), e.poll("Order.Created", b, 50).stream().map(Event::position).toList());
            assertTrue(e.poll("Order.Created", c, 50).isEmpty());
            assertThrows(ConnectorException.Invalid.class, () -> e.poll("Order.Paid", 0, 10));
        }
    }

    @Test
    void ordersAndCustomersAreReadByNumberOrGid() throws Exception {
        long a = order("ada@example.com", "349.90");
        try (Endpoint e = bind()) {
            JsonNode byNumber = e.read("get-order", Long.toString(a)), byGid = e.read("get-order", "gid://shopify/Order/" + a);
            assertEquals(byNumber, byGid);
            String customer = byNumber.path("customer").path("admin_graphql_api_id").asText();
            assertEquals("Lovelace", e.read("get-customer", customer).path("last_name").asText());
            assertThrows(ConnectorException.NotFound.class, () -> e.read("get-order", "1"));
            assertThrows(ConnectorException.Invalid.class, () -> e.read("get-order", "#1001"));
        }
    }

    @Test
    void anUpdateIsPreviewedCommittedConfirmedAndRestored() throws Exception {
        long a = order("ada@example.com", "349.90");
        try (Endpoint e = bind()) {
            JsonNode change = json("{\"orderId\": \"" + a + "\", \"note\": \"ERP order 12000\", \"tags\": \"erp, synced\"}");
            JsonNode prev = e.simulate("update-order", change);
            assertTrue(prev.path("current").path("note").isNull());
            assertEquals("ERP order 12000", prev.path("proposed").path("note").asText());
            assertTrue(shop.order(a).path("note").isNull());

            JsonNode res = e.commit("update-order", "K", change);
            assertEquals("ERP order 12000", shop.order(a).path("note").asText());
            assertEquals(List.of("erp", "synced"), List.of(shop.order(a).path("tags").get(0).asText(), shop.order(a).path("tags").get(1).asText()));
            assertTrue(res.path("previous").path("note").isNull());
            e.confirm("update-order", res);

            // The saga compensates with update-order's result.
            e.commit("restore-order", "K#compensate", res);
            assertTrue(shop.order(a).path("note").isNull());
            assertEquals(0, shop.order(a).path("tags").size());
            assertThrows(ConnectorException.Rejected.class, () -> e.confirm("update-order", res));
        }
    }

    @Test
    void userErrorsRejectTheWrite() throws Exception {
        long a = order("ada@example.com", "349.90");
        shop.rejectUpdate(a);
        try (Endpoint e = bind()) {
            var x = assertThrows(ConnectorException.Rejected.class, () -> e.commit("update-order", "K", json("{\"orderId\": " + a + ", \"note\": \"n\"}")));
            assertTrue(x.getMessage().contains("archived"), x.getMessage());
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("update-order", "K", json("{\"orderId\": " + a + "}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("update-order", "K", json("{\"orderId\": \"abc\", \"note\": \"n\"}")));
            assertThrows(ConnectorException.NotFound.class, () -> e.commit("update-order", "K", json("{\"orderId\": 1, \"note\": \"n\"}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("restore-order", "K", json("{\"orderId\": " + a + ", \"note\": \"n\"}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("delete-order", "K", json("{\"orderId\": " + a + ", \"note\": \"n\"}")));
        }
    }

    @Test
    void throttlingIsRetriedAndMissingScopesAreNot() throws Exception {
        order("ada@example.com", "1.00");
        try (Endpoint e = bind()) {
            shop.throttle(1);
            Exception x = assertThrows(Exception.class, () -> e.poll("Order.Created", 0, 10));
            assertFalse(x instanceof ConnectorException, "throttling must be transient, got " + x);
            assertEquals(1, e.poll("Order.Created", 0, 10).size());
        }
        try (FakeShopify ro = fake(Set.of("read_orders")); Endpoint e = bind(ro, "2026-07", "shpat_demo")) {
            long a = ro.addOrder(json("{\"email\": \"a@example.com\", \"subtotal\": \"1.00\"}"));
            var x = assertThrows(ConnectorException.Rejected.class, () -> e.commit("update-order", "K", json("{\"orderId\": " + a + ", \"note\": \"n\"}")));
            assertTrue(x.getMessage().contains("write_orders"), x.getMessage());
        }
        try (Endpoint e = bind(shop, "2026-07", "shpat_wrong")) {
            assertInstanceOf(IOException.class, assertThrows(Exception.class, () -> e.poll("Order.Created", 0, 10)));
        }
    }

    @Test
    void discoverReadsTheSchemaByIntrospection() throws Exception {
        try (Endpoint e = bind()) {
            JsonNode cat = e.discover(List.of("Refund"));
            assertEquals(2, cat.path("objects").size(), cat.toString()); // the fake has no Refund
            JsonNode order = cat.path("objects").get(0);
            assertEquals("Order", order.path("name").asText());
            JsonNode created = null, note = null;
            for (JsonNode f : order.path("fields")) {
                if (f.path("name").asText().equals("createdAt")) {
                    created = f;
                }
                if (f.path("name").asText().equals("note")) {
                    note = f;
                }
            }
            assertTrue(created.path("required").asBoolean() && created.path("type").asText().equals("DateTime"), String.valueOf(created));
            assertFalse(note.path("required").asBoolean());
            assertTrue(cat.path("uses").size() > 20);
        }
    }

    private Endpoint bindWithWebhook() throws Exception {
        JsonNode cfg = json("{\"shop\": \"turgon-demo.myshopify.com\", \"apiVersion\": \"2026-07\", \"url\": \"" + shop.url()
                + "\", \"webhook\": {\"secretRef\": \"openbao://shopify-admin/client-secret\"}}");
        return new ShopifyConnector().bind(new EndpointConfig("shopify-admin", "shopify", "2.1.0", cfg, "shpat_demo",
                Map.of("openbao://shopify-admin/client-secret", "shpss_client_secret")));
    }

    private static String hmac(String secret, byte[] body) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        return java.util.Base64.getEncoder().encodeToString(mac.doFinal(body));
    }

    private static Map<String, List<String>> headers(String sig, String topic) {
        Map<String, List<String>> h = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (sig != null) {
            h.put("X-Shopify-Hmac-Sha256", List.of(sig));
        }
        h.put("X-Shopify-Topic", List.of(topic));
        h.put("X-Shopify-Shop-Domain", List.of("turgon-demo.myshopify.com"));
        return h;
    }

    @Test
    void ordersCreateWebhooksAreVerifiedAndKeyedLikePolledOrders() throws Exception {
        long a = order("ada@example.com", "349.90");
        try (Endpoint e = bindWithWebhook(); Endpoint plain = bind()) {
            assertEquals(Set.of("Order.Created"), e.webhooks());
            assertTrue(plain.webhooks().isEmpty());
            // The webhook's body is the REST order; the poll's payload has that shape.
            byte[] body = JSON.writeValueAsBytes(e.poll("Order.Created", 0, 1).get(0).payload());
            List<Event> evs = e.receive("Order.Created", headers(hmac("shpss_client_secret", body), "orders/create"), body, java.time.Instant.now());
            assertEquals(1, evs.size());
            assertEquals("gid://shopify/Order/" + a, evs.get(0).id());
            assertEquals("349.90", evs.get(0).payload().path("subtotal_price").asText());

            assertThrows(ConnectorException.Unauthenticated.class,
                    () -> e.receive("Order.Created", headers(hmac("wrong", body), "orders/create"), body, java.time.Instant.now()));
            assertThrows(ConnectorException.Unauthenticated.class, () -> e.receive("Order.Created", headers(null, "orders/create"), body, java.time.Instant.now()));
            byte[] tampered = new String(body, java.nio.charset.StandardCharsets.UTF_8).replace("349.90", "1.00").getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThrows(ConnectorException.Unauthenticated.class,
                    () -> e.receive("Order.Created", headers(hmac("shpss_client_secret", body), "orders/create"), tampered, java.time.Instant.now()));
            assertTrue(e.receive("Order.Created", headers(hmac("shpss_client_secret", body), "orders/paid"), body, java.time.Instant.now()).isEmpty());
        }
        JsonNode noSecret = json("{\"shop\": \"turgon-demo.myshopify.com\", \"apiVersion\": \"2026-07\", \"webhook\": {\"secretRef\": \"openbao://x\"}}");
        // Without its secret the webhook is reported, not fatal: the orders are still polled.
        try (var e = new ShopifyConnector().bind(new EndpointConfig("s", "shopify", "2.1.0", noSecret, "t", Map.of()))) {
            assertTrue(e.webhooks().isEmpty());
            assertTrue(e.webhookErrors().get("Order.Created").contains("openbao://x is not set"));
        }
        JsonNode noRef = json("{\"shop\": \"turgon-demo.myshopify.com\", \"apiVersion\": \"2026-07\", \"webhook\": {}}");
        assertThrows(ConnectorException.Invalid.class, () -> new ShopifyConnector().bind(new EndpointConfig("s", "shopify", "2.1.0", noRef, "t", Map.of())));
    }

    @Test
    void configurationErrorsAreInvalid() {
        for (String cfg : List.of("{\"shop\": \"turgon-demo.com\", \"apiVersion\": \"2026-07\"}", "{\"shop\": \"turgon-demo.myshopify.com\", \"apiVersion\": \"2026-08\"}",
                "{\"shop\": \"turgon-demo.myshopify.com\", \"apiVersion\": \"2026-07\", \"pageSize\": 500}")) {
            assertThrows(ConnectorException.Invalid.class, () -> new ShopifyConnector()
                    .bind(new EndpointConfig("s", "shopify", "2.1.0", (ObjectNode) json(cfg), "t", Map.of())), cfg);
        }
    }
}
