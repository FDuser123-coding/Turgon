package dev.turgon.connector.stripe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import dev.turgon.connector.stripe.fake.FakeStripe;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StripeEndpointTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> ALL = Set.of("customers:read", "invoices:read", "invoices:write", "events:read");
    private static final String KEY = "rk_test_51Demo0000000000000000000000";

    private FakeStripe stripe;
    private String ada;

    @BeforeEach
    void start() throws Exception {
        stripe = new FakeStripe(0, KEY, ALL);
        ada = stripe.addCustomer("ada@example.com", "Ada Lovelace GmbH");
    }

    @AfterEach
    void stop() {
        stripe.close();
    }

    private static Endpoint bind(FakeStripe s, String key, ObjectNode extra) throws Exception {
        ObjectNode cfg = JSON.createObjectNode().put("url", s.url());
        if (extra != null) {
            cfg.setAll(extra);
        }
        return new StripeConnector().bind(new EndpointConfig("stripe-invoicing", "stripe", "1.5.0", cfg, key, Map.of()));
    }

    private Endpoint bind() throws Exception {
        return bind(stripe, KEY, null);
    }

    private static JsonNode json(String s) throws IOException {
        return JSON.readTree(s);
    }

    private JsonNode order() throws IOException {
        return json("""
                {"customerId": "%s", "currency": "EUR", "memo": "Order SO-1",
                 "lines": [{"description": "Widget", "amount": "174.95", "quantity": 2}, {"description": "Shipping", "amount": 12}]}""".formatted(ada));
    }

    @Test
    void checksReportTheModeAndWhatTheKeyMayRead() throws Exception {
        try (Endpoint e = bind()) {
            List<CheckResult> checks = e.check();
            for (CheckResult c : checks) {
                assertTrue(c.ok(), c.toString());
            }
            assertTrue(checks.get(0).detail().startsWith("test mode"), checks.get(0).detail());
        }
        try (Endpoint e = bind(stripe, "rk_test_wrong000000000000", null)) {
            CheckResult c = e.check().get(1);
            assertFalse(c.ok());
            assertTrue(c.fix().contains("rolled"), c.fix());
        }
        try (FakeStripe narrow = new FakeStripe(0, KEY, Set.of("customers:read", "invoices:read", "invoices:write")); Endpoint e = bind(narrow, KEY, null)) {
            CheckResult ev = e.check().get(3);
            assertFalse(ev.ok());
            assertTrue(ev.detail().contains("rak_event_read") && ev.fix().contains("Events: Read"), ev.toString());
        }
        try (Endpoint e = bind(stripe, "pk_test_publishable", null)) {
            assertFalse(e.check().get(0).ok());
        }
    }

    @Test
    void anInvoiceIsPreviewedThenCreatedOnceAndVoided() throws Exception {
        try (Endpoint e = bind()) {
            JsonNode prev = e.simulate("create-invoice", order());
            assertEquals(new BigDecimal("361.90"), prev.path("total").decimalValue());
            assertEquals("Widget × 2", prev.path("lines").get(0).path("description").asText());
            assertTrue(stripe.invoices().isEmpty());

            JsonNode res = e.commit("create-invoice", "SO-1", order());
            assertEquals("open", res.path("status").asText());
            assertEquals("TURG-0001", res.path("number").asText());
            assertEquals(new BigDecimal("361.90"), res.path("total").decimalValue());
            ObjectNode inv = stripe.invoice(res.path("id").asText());
            assertEquals(36190, inv.path("total").asLong());
            assertEquals(2, inv.path("lines").path("data").size());
            assertEquals("SO-1", inv.path("metadata").path("turgon_key").asText());
            assertEquals("send_invoice", inv.path("collection_method").asText());
            e.confirm("create-invoice", res);

            // A retried create returns the same invoice: Stripe replays each idempotent request.
            JsonNode again = e.commit("create-invoice", "SO-1", order());
            assertEquals(res.path("id").asText(), again.path("id").asText());
            assertEquals(1, stripe.invoices().size());
            assertEquals(2, stripe.invoice(res.path("id").asText()).path("lines").path("data").size());

            // The saga compensates with create-invoice's result.
            assertEquals("void", e.commit("void-invoice", "SO-1#compensate", res).path("status").asText());
            assertEquals("void", stripe.invoice(res.path("id").asText()).path("status").asText());
            assertEquals("void", e.commit("void-invoice", "SO-1#compensate", res).path("status").asText());
        }
    }

    @Test
    void aDraftIsDeletedAndAPaidInvoiceIsNotVoided() throws Exception {
        try (Endpoint e = bind(stripe, KEY, (ObjectNode) json("{\"invoice\": {\"finalize\": false}}"))) {
            JsonNode res = e.commit("create-invoice", "D-1", order());
            assertEquals("draft", res.path("status").asText());
            assertEquals("deleted", e.commit("void-invoice", "D-1#compensate", res).path("status").asText());
            assertNull(stripe.invoice(res.path("id").asText()));
        }
        try (Endpoint e = bind()) {
            JsonNode res = e.commit("create-invoice", "P-1", order());
            stripe.pay(res.path("id").asText());
            var x = assertThrows(ConnectorException.Rejected.class, () -> e.commit("void-invoice", "P-1#compensate", res));
            assertTrue(x.getMessage().contains("refund"), x.getMessage());
        }
    }

    @Test
    void paidInvoicesArePolledOldestFirstAcrossPages() throws Exception {
        try (Endpoint e = bind(stripe, KEY, (ObjectNode) json("{\"pageSize\": 1}"))) {
            String[] ids = new String[3];
            for (int i = 0; i < 3; i++) {
                ids[i] = e.commit("create-invoice", "K" + i, order()).path("id").asText();
                stripe.pay(ids[i]);
            }
            List<Event> first = e.poll("Invoice.Paid", 0, 2);
            assertEquals(2, first.size());
            assertEquals(ids[0], first.get(0).payload().path("data").path("object").path("id").asText());
            assertEquals(ids[1], first.get(1).payload().path("data").path("object").path("id").asText());
            assertTrue(first.get(0).position() < first.get(1).position());
            assertEquals("invoice.paid", first.get(0).payload().path("type").asText());
            assertEquals(36190, first.get(0).payload().path("data").path("object").path("amount_paid").asLong());
            List<Event> rest = e.poll("Invoice.Paid", first.get(1).position(), 10);
            assertEquals(1, rest.size());
            assertEquals(ids[2], rest.get(0).payload().path("data").path("object").path("id").asText());
            assertTrue(e.poll("Invoice.Paid", rest.get(0).position(), 10).isEmpty());
            assertThrows(ConnectorException.Invalid.class, () -> e.poll("Invoice.Created", 0, 10));
        }
    }

    @Test
    void aLinkIsPreviewedSetConfirmedAndUndone() throws Exception {
        try (Endpoint e = bind()) {
            String id = e.commit("create-invoice", "L-1", order()).path("id").asText();
            JsonNode link = json("{\"invoiceId\": \"" + id + "\", \"erpPaymentId\": \"7\"}");
            JsonNode prev = e.simulate("link-invoice", link);
            assertTrue(prev.path("current").path("erp_payment_id").isNull());
            assertEquals("7", prev.path("proposed").path("erp_payment_id").asText());
            JsonNode res = e.commit("link-invoice", "L-1/link", link);
            assertEquals("7", stripe.invoice(id).path("metadata").path("erp_payment_id").asText());
            e.confirm("link-invoice", res);
            e.commit("unlink-invoice", "L-1/link#compensate", res);
            assertFalse(stripe.invoice(id).path("metadata").has("erp_payment_id"));
        }
    }

    private static String sign(String secret, long t, byte[] body) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        mac.update((t + ".").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return java.util.HexFormat.of().formatHex(mac.doFinal(body));
    }

    private static Map<String, List<String>> sig(String value) {
        Map<String, List<String>> h = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        h.put("Stripe-Signature", List.of(value));
        return h;
    }

    @Test
    void webhookDeliveriesAreVerifiedWithinTheTolerance() throws Exception {
        ObjectNode extra = (ObjectNode) json("{\"webhook\": {\"secretRef\": \"openbao://stripe-invoicing/webhook-secret\"}}");
        ObjectNode cfg = JSON.createObjectNode().put("url", stripe.url());
        cfg.setAll(extra);
        try (Endpoint e = new StripeConnector().bind(new EndpointConfig("stripe-invoicing", "stripe", "1.5.0", cfg, KEY,
                Map.of("openbao://stripe-invoicing/webhook-secret", "whsec_test")))) {
            assertEquals(Set.of("Invoice.Paid"), e.webhooks());
            String id = e.commit("create-invoice", "W-1", order()).path("id").asText();
            JsonNode paid = stripe.pay(id);
            byte[] body = JSON.writeValueAsBytes(paid);
            java.time.Instant now = java.time.Instant.now();
            long t = now.getEpochSecond();
            List<Event> evs = e.receive("Invoice.Paid", sig("t=" + t + ",v1=" + sign("whsec_other", t, body) + ",v1=" + sign("whsec_test", t, body)), body, now);
            assertEquals(1, evs.size());
            // The same ID the poll gives, so the worker runs it once.
            assertEquals(e.poll("Invoice.Paid", 0, 10).get(0).id(), evs.get(0).id());

            assertThrows(ConnectorException.Unauthenticated.class, () -> e.receive("Invoice.Paid", sig("t=" + t + ",v1=" + sign("whsec_other", t, body)), body, now));
            assertThrows(ConnectorException.Unauthenticated.class,
                    () -> e.receive("Invoice.Paid", sig("t=" + (t - 600) + ",v1=" + sign("whsec_test", t - 600, body)), body, now));
            assertThrows(ConnectorException.Unauthenticated.class, () -> e.receive("Invoice.Paid", Map.of(), body, now));
            byte[] other = JSON.writeValueAsBytes(((ObjectNode) paid.deepCopy()).put("type", "invoice.finalized"));
            assertTrue(e.receive("Invoice.Paid", sig("t=" + t + ",v1=" + sign("whsec_test", t, other)), other, now).isEmpty());
        }
        ObjectNode noSecret = JSON.createObjectNode().put("url", stripe.url());
        noSecret.putObject("webhook").put("secretRef", "openbao://missing");
        // Without its secret the webhook is reported, not fatal: the invoices are still polled.
        try (var e = new StripeConnector().bind(new EndpointConfig("s", "stripe", "1.5.0", noSecret, KEY, Map.of()))) {
            assertTrue(e.webhooks().isEmpty());
            assertTrue(e.webhookErrors().get("Invoice.Paid").contains("openbao://missing is not set"));
        }
        ObjectNode noRef = JSON.createObjectNode().put("url", stripe.url());
        noRef.putObject("webhook");
        assertThrows(ConnectorException.Invalid.class, () -> new StripeConnector().bind(new EndpointConfig("s", "stripe", "1.5.0", noRef, KEY, Map.of())));
    }

    @Test
    void amountsFollowTheCurrencysMinorUnit() throws Exception {
        assertEquals(1500, StripeEndpoint.minor(new BigDecimal("1500"), "JPY"));
        assertEquals(1500, StripeEndpoint.minor(new BigDecimal("1.5"), "KWD"));
        assertEquals(1999, StripeEndpoint.minor(new BigDecimal("19.99"), "eur"));
        assertThrows(ConnectorException.Invalid.class, () -> StripeEndpoint.minor(new BigDecimal("19.999"), "eur"));
        assertThrows(ConnectorException.Invalid.class, () -> StripeEndpoint.minor(new BigDecimal("1.5"), "jpy"));
    }

    @Test
    void invalidRequestsAreNotRetriedAndThrottlingIs() throws Exception {
        try (Endpoint e = bind()) {
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("create-invoice", "X", json("{\"customerId\": \"cus_404\", \"currency\": \"eur\", \"lines\": [{\"description\": \"a\", \"amount\": 1}]}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("create-invoice", "X", json("{\"customerId\": \"" + ada + "\", \"currency\": \"eur\", \"lines\": []}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("create-invoice", "X", json("{\"customerId\": \"bob\", \"currency\": \"eur\", \"lines\": [{\"description\": \"a\", \"amount\": 1}]}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("refund-invoice", "X", order()));
            assertThrows(ConnectorException.NotFound.class, () -> e.read("get-invoice", "in_404"));
            assertEquals("ada@example.com", e.read("get-customer", ada).path("email").asText());
            stripe.throttle(1);
            Exception x = assertThrows(Exception.class, () -> e.read("get-customer", ada));
            assertFalse(x instanceof ConnectorException, "a rate limit must be transient, got " + x);
            assertTrue(x.getMessage().contains("429"), x.getMessage());
        }
        try (FakeStripe ro = new FakeStripe(0, KEY, Set.of("customers:read", "invoices:read", "events:read")); Endpoint e = bind(ro, KEY, null)) {
            String cus = ro.addCustomer("a@example.com", "A");
            var x = assertThrows(ConnectorException.Rejected.class, () -> e.commit("create-invoice", "X",
                    json("{\"customerId\": \"" + cus + "\", \"currency\": \"eur\", \"lines\": [{\"description\": \"a\", \"amount\": 1}]}")));
            assertTrue(x.getMessage().contains("rak_invoice_write"), x.getMessage());
        }
        try (Endpoint e = bind(stripe, "rk_test_wrong000000000000", null)) {
            assertInstanceOf(IOException.class, assertThrows(Exception.class, () -> e.read("get-customer", ada)));
        }
    }
}
