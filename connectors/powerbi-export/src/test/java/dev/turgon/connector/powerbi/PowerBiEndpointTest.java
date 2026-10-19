package dev.turgon.connector.powerbi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.powerbi.fake.FakePowerBi;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PowerBiEndpointTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> COLUMNS = new LinkedHashSet<>(
            List.of("OrderId", "Customer", "OrderDate", "NetValue", "Lines", "Reversal", "TurgonKey"));

    private FakePowerBi pbi;

    @BeforeEach
    void start() throws Exception {
        pbi = new FakePowerBi(0, "contoso", "app-1", "s3cret", "ws-1", "ds-1", Map.of("SalesOrders", COLUMNS));
    }

    @AfterEach
    void stop() {
        pbi.close();
    }

    private ObjectNode config() throws Exception {
        return (ObjectNode) JSON.readTree("""
                {"tenant": "contoso", "clientId": "app-1", "workspace": "ws-1", "dataset": "ds-1",
                 "authority": "%s", "api": "%s",
                 "tables": {"SalesOrders": {
                   "columns": {"OrderId": "String", "Customer": "String", "OrderDate": "DateTime", "NetValue": "Double",
                               "Lines": "Int64", "Reversal": "Boolean", "TurgonKey": "String"},
                   "key": "TurgonKey",
                   "reverse": {"negate": ["NetValue", "Lines"], "set": {"Reversal": true}}}}}
                """.formatted(pbi.url(), pbi.url()));
    }

    private Endpoint bind(JsonNode config, String secret) throws Exception {
        return new PowerBiConnector().bind(new EndpointConfig("powerbi-sales", "powerbi-export", "1.1.0", config, secret, Map.of()));
    }

    private Endpoint bind() throws Exception {
        return bind(config(), "s3cret");
    }

    private static JsonNode json(String s) throws IOException {
        return JSON.readTree(s);
    }

    private static final String ORDER = """
            {"OrderId": "SO-1", "Customer": "C-100", "OrderDate": "2026-10-01", "NetValue": 1250.5, "Lines": 2}""";

    @Test
    void checksPassWhenTheDatasetAndTablesAreThere() throws Exception {
        try (Endpoint e = bind()) {
            List<CheckResult> checks = e.check();
            for (CheckResult c : checks) {
                assertTrue(c.ok(), c.toString());
            }
            assertEquals(List.of("sign-in", "dataset", "table SalesOrders", "reverse SalesOrders"), checks.stream().map(CheckResult::name).toList());
            assertTrue(checks.get(1).detail().contains("push dataset ds-1"), checks.get(1).detail());
        }
    }

    @Test
    void simulateChecksTheRowAndSendsNothing() throws Exception {
        try (Endpoint e = bind()) {
            JsonNode prev = e.simulate("push-rows", json(ORDER));
            assertTrue(prev.path("simulated").asBoolean());
            JsonNode row = prev.path("rows").get(0);
            assertEquals("2026-10-01T00:00:00", row.path("OrderDate").asText());
            assertFalse(row.has("TurgonKey"));
            assertEquals(0, pbi.rows("SalesOrders").size());
        }
    }

    @Test
    void pushAddsTheRowWithItsKeyAndReverseUndoesItsSums() throws Exception {
        try (Endpoint e = bind()) {
            JsonNode res = e.commit("push-rows", "SHOP-1", json(ORDER));
            assertEquals(1, res.path("count").asInt());
            List<JsonNode> rows = pbi.rows("SalesOrders");
            assertEquals(1, rows.size());
            assertEquals("SHOP-1", rows.get(0).path("TurgonKey").asText());
            assertEquals("C-100", rows.get(0).path("Customer").asText());

            // The saga compensates with push-rows' result.
            e.commit("reverse-rows", "SHOP-1#compensate", res);
            rows = pbi.rows("SalesOrders");
            assertEquals(2, rows.size());
            JsonNode rev = rows.get(1);
            assertEquals("SHOP-1#reversed", rev.path("TurgonKey").asText());
            assertTrue(rev.path("Reversal").asBoolean());
            assertEquals("SO-1", rev.path("OrderId").asText());
            BigDecimal net = BigDecimal.ZERO;
            long lines = 0;
            for (JsonNode r : rows) {
                net = net.add(r.path("NetValue").decimalValue());
                lines += r.path("Lines").asLong();
            }
            assertEquals(0, net.signum());
            assertEquals(0, lines);
        }
    }

    @Test
    void severalRowsGetNumberedKeys() throws Exception {
        try (Endpoint e = bind()) {
            e.commit("push-rows", "K", json("""
                    {"table": "SalesOrders", "rows": [{"OrderId": "A", "NetValue": "10.25"}, {"OrderId": "B", "NetValue": 3}]}"""));
            List<JsonNode> rows = pbi.rows("SalesOrders");
            assertEquals(List.of("K/1", "K/2"), rows.stream().map(r -> r.path("TurgonKey").asText()).toList());
            assertEquals(new BigDecimal("10.25"), rows.get(0).path("NetValue").decimalValue());
        }
    }

    @Test
    void largePushesGoInBatchesOfTenThousand() throws Exception {
        ObjectNode p = JSON.createObjectNode();
        ArrayNode rows = p.putArray("rows");
        for (int i = 0; i < PowerBiEndpoint.BATCH + 1; i++) {
            rows.addObject().put("OrderId", "O-" + i).put("NetValue", 1);
        }
        try (Endpoint e = bind()) {
            assertEquals(PowerBiEndpoint.BATCH + 1, e.commit("push-rows", "BIG", p).path("count").asInt());
        }
        assertEquals(2, pbi.pushes());
        assertEquals(PowerBiEndpoint.BATCH + 1, pbi.rows("SalesOrders").size());
    }

    @Test
    void rowsTheTableCannotTakeAreInvalidAndNameNoValues() throws Exception {
        try (Endpoint e = bind()) {
            var unknown = assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json("{\"Discount\": 5}")));
            assertTrue(unknown.getMessage().contains("column Discount is not in table SalesOrders"), unknown.getMessage());
            var type = assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json("{\"Lines\": \"two-thousand\"}")));
            assertTrue(type.getMessage().contains("column Lines must be Int64") && !type.getMessage().contains("two-thousand"), type.getMessage());
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json("{\"OrderDate\": \"yesterday\"}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json("{\"table\": \"Invoices\", \"rows\": [{}]}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json("{\"rows\": []}")));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("drop-table", "K", json("{}")));
            assertEquals(0, pbi.rows("SalesOrders").size());
        }
    }

    @Test
    void throttlingIsRetriedByTheWorker() throws Exception {
        try (Endpoint e = bind()) {
            pbi.throttle(1);
            Exception x = assertThrows(Exception.class, () -> e.commit("push-rows", "K", json(ORDER)));
            assertFalse(x instanceof ConnectorException, "429 must be transient, got " + x);
            assertTrue(x.getMessage().contains("429") && x.getMessage().contains("retry after 1 s"), x.getMessage());
            e.commit("push-rows", "K", json(ORDER));
            assertEquals(1, pbi.rows("SalesOrders").size());
        }
    }

    @Test
    void anExpiredTokenIsRenewed() throws Exception {
        try (Endpoint e = bind()) {
            e.commit("push-rows", "K1", json(ORDER));
            pbi.expireTokens();
            e.commit("push-rows", "K2", json(ORDER));
            assertEquals(2, pbi.tokensIssued());
            assertEquals(2, pbi.rows("SalesOrders").size());
        }
    }

    @Test
    void aWrongSecretFailsTheCheckWithAFix() throws Exception {
        try (Endpoint e = bind(config(), "wrong")) {
            List<CheckResult> checks = e.check();
            assertEquals(1, checks.size());
            CheckResult c = checks.get(0);
            assertFalse(c.ok());
            assertTrue(c.detail().contains("AADSTS7000215") && !c.detail().contains("wrong"), c.detail());
            assertTrue(c.fix().contains("client secret is wrong or expired"), c.fix());
            Exception x = assertThrows(Exception.class, () -> e.commit("push-rows", "K", json(ORDER)));
            assertInstanceOf(IOException.class, x); // retried: a rotated secret recovers
        }
    }

    @Test
    void aMissingDatasetFailsTheCheckAndWritesAreInvalid() throws Exception {
        ObjectNode cfg = config().put("dataset", "ds-404");
        try (Endpoint e = bind(cfg, "s3cret")) {
            CheckResult c = e.check().get(1);
            assertFalse(c.ok());
            assertTrue(c.fix().contains("config.dataset"), c.fix());
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json(ORDER)));
        }
    }

    @Test
    void aTableWithoutReverseFailsItsCheck() throws Exception {
        ObjectNode cfg = config();
        ((ObjectNode) cfg.path("tables").path("SalesOrders")).remove("reverse");
        try (Endpoint e = bind(cfg, "s3cret")) {
            CheckResult rev = e.check().get(3);
            assertFalse(rev.ok());
            assertTrue(rev.fix().contains("reverse.negate"), rev.fix());
            JsonNode res = e.commit("push-rows", "K", json(ORDER));
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("reverse-rows", "K#compensate", res));
        }
    }

    @Test
    void deleteRowsEmptiesTheTable() throws Exception {
        try (Endpoint e = bind()) {
            e.commit("push-rows", "K", json(ORDER));
            assertTrue(e.simulate("delete-rows", json("{}")).path("deletesAllRows").asBoolean());
            e.commit("delete-rows", "D", json("{}"));
            assertEquals(0, pbi.rows("SalesOrders").size());
        }
    }

    @Test
    void discoverDescribesTheConfiguredTables() throws Exception {
        ObjectNode cfg = config();
        ((ObjectNode) cfg.path("tables")).set("Invoices", json("{\"columns\": {\"InvoiceId\": \"String\"}}"));
        cfg.put("table", "SalesOrders");
        try (Endpoint e = bind(cfg, "s3cret")) {
            JsonNode cat = e.discover(List.of());
            assertEquals(1, cat.path("objects").size(), cat.toString()); // Invoices is not in the dataset
            JsonNode o = cat.path("objects").get(0);
            assertEquals("SalesOrders", o.path("name").asText());
            assertEquals("DateTime", o.path("fields").get(2).path("type").asText());
            assertTrue(o.path("fields").get(6).path("key").asBoolean());
            assertEquals(COLUMNS.size(), cat.path("uses").size());
            assertTrue(cat.path("uses").get(0).path("creates").asBoolean());
        }
    }

    @Test
    void configurationErrorsAreInvalid() throws Exception {
        assertThrows(ConnectorException.Invalid.class, () -> bind(config().put("tenant", ""), "s3cret"));
        ObjectNode key = config();
        ((ObjectNode) key.path("tables").path("SalesOrders")).put("key", "NetValue");
        assertTrue(assertThrows(ConnectorException.Invalid.class, () -> bind(key, "s3cret")).getMessage().contains("String columns"));
        ObjectNode neg = config();
        ((ObjectNode) neg.path("tables").path("SalesOrders").path("reverse")).set("negate", json("[\"OrderId\"]"));
        assertTrue(assertThrows(ConnectorException.Invalid.class, () -> bind(neg, "s3cret")).getMessage().contains("numeric columns"));
        ObjectNode type = config();
        ((ObjectNode) type.path("tables").path("SalesOrders").path("columns")).put("Lines", "Integer");
        assertThrows(ConnectorException.Invalid.class, () -> bind(type, "s3cret"));
        ObjectNode two = config();
        ((ObjectNode) two.path("tables")).set("Invoices", json("{\"columns\": {\"InvoiceId\": \"String\"}}"));
        try (Endpoint e = bind(two, "s3cret")) { // two tables, no default: the payload must name one
            assertThrows(ConnectorException.Invalid.class, () -> e.commit("push-rows", "K", json(ORDER)));
        }
    }
}
