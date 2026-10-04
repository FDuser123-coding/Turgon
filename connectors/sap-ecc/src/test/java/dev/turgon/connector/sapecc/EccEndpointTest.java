package dev.turgon.connector.sapecc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EccEndpointTest {
    static final ObjectMapper JSON = new ObjectMapper();
    static final String ORDER = """
            {"externalId":"006A","customerId":"1000","orderDate":"2026-10-01","currency":"eur",
             "lines":[{"material":"m-7","quantity":2},{"material":"M-8","quantity":"1.5"}]}""";

    Endpoint ecc;
    FakeEcc sap;

    static EndpointConfig config(String json, String secret) throws IOException {
        return new EndpointConfig("sap-ecc", "sap-ecc", "0.4.0", JSON.readTree(json), secret, Map.of());
    }

    @BeforeEach
    void bind() throws Exception {
        FakeEcc.reset();
        ecc = new SapEccConnector().bind(config("{\"rfc\":{\"provider\":\"fake\",\"destination\":{\"ashost\":\"ecc1\",\"client\":\"100\"}}}", "TURGON:pw"));
        sap = FakeEcc.system("ecc1");
    }

    @AfterEach
    void close() throws Exception {
        ecc.close();
    }

    static JsonNode json(String s) throws IOException {
        return JSON.readTree(s);
    }

    @Test
    void simulatesCreatesAndConfirms() throws Exception {
        JsonNode preview = ecc.simulate("create-sales-order", json(ORDER));
        assertTrue(preview.path("testRun").asBoolean());
        assertEquals("0000001000", preview.path("customer").asText());
        assertEquals(2, preview.path("items").asInt());
        assertTrue(sap.orders().isEmpty(), "a test run saves nothing");

        JsonNode created = ecc.commit("create-sales-order", "006A", json(ORDER));
        String number = created.path("salesOrder").asText();
        assertEquals("0000012000", number);
        FakeEcc.Order o = sap.orders().get(number);
        assertEquals("006A", o.purchaseOrder());
        assertEquals("20261001", o.date());
        assertEquals("M-7", o.items().getFirst().get("MATERIAL"));
        assertEquals("1.5", o.items().get(1).get("TARGET_QTY"));
        ecc.confirm("create-sales-order", created);

        // A retried create finds the order by its purchase order number.
        JsonNode again = ecc.commit("create-sales-order", "006A", json(ORDER));
        assertEquals(number, again.path("salesOrder").asText());
        assertTrue(again.path("existing").asBoolean());
        assertEquals(1, sap.orders().size());

        assertThrows(ConnectorException.NotFound.class, () -> ecc.confirm("create-sales-order", json("{\"salesOrder\":\"0000099999\"}")));
    }

    @Test
    void sapRefusalsAreRejections() throws Exception {
        var blocked = assertThrows(ConnectorException.Rejected.class,
                () -> ecc.commit("create-sales-order", "K1", json(ORDER.replace("\"1000\"", "\"1002\""))));
        assertTrue(blocked.getMessage().contains("V1 462"), blocked.getMessage());
        assertThrows(ConnectorException.Rejected.class,
                () -> ecc.simulate("create-sales-order", json(ORDER.replace("m-7", "X-1"))));
        assertTrue(sap.orders().isEmpty());
        assertTrue(sap.calls.contains("BAPI_TRANSACTION_ROLLBACK"));
    }

    @Test
    void invalidPayloads() throws Exception {
        for (String bad : List.of("[]", "{\"lines\":[{\"material\":\"M-7\",\"quantity\":1}]}",
                ORDER.replace("\"1000\"", "\"12345678901\""), ORDER.replace("2026-10-01", "1 Oct"),
                ORDER.replace("\"quantity\":2", "\"quantity\":0"), ORDER.replace("\"quantity\":2", "\"quantity\":\"two\""),
                "{\"customerId\":\"1000\",\"lines\":[]}")) {
            assertThrows(ConnectorException.Invalid.class, () -> ecc.commit("create-sales-order", "K1", json(bad)), bad);
        }
        assertThrows(ConnectorException.Invalid.class, () -> ecc.commit("create-sales-order", "x".repeat(36), json(ORDER)));
        assertThrows(ConnectorException.Invalid.class, () -> ecc.commit("nope", "K1", json(ORDER)));
        assertThrows(ConnectorException.Unsupported.class, () -> ecc.simulate("cancel-sales-order", json("{}")));
    }

    @Test
    void cancelsAsCompensation() throws Exception {
        JsonNode created = ecc.commit("create-sales-order", "K1", json(ORDER));
        JsonNode cancelled = ecc.commit("cancel-sales-order", "K1#compensate", created);
        assertTrue(cancelled.path("deleted").asBoolean());
        assertTrue(sap.orders().isEmpty());
        assertTrue(ecc.commit("cancel-sales-order", "K1#compensate", created).path("alreadyGone").asBoolean());

        JsonNode delivered = ecc.commit("create-sales-order", "K2", json(ORDER));
        sap.deliver(delivered.path("salesOrder").asText());
        var e = assertThrows(ConnectorException.Rejected.class, () -> ecc.commit("cancel-sales-order", "K2#compensate", delivered));
        assertTrue(e.getMessage().contains("subsequent documents"), e.getMessage());
        assertEquals(1, sap.orders().size());
        assertThrows(ConnectorException.Invalid.class, () -> ecc.commit("cancel-sales-order", "K3", json("{}")));
    }

    @Test
    void readsCustomers() throws Exception {
        JsonNode c = ecc.read("get-customer", "1000");
        assertEquals("Ada Lovelace GmbH", c.path("name").asText());
        assertEquals("Berlin", c.path("city").asText());
        assertEquals("0000001000", c.path("id").asText());
        assertThrows(ConnectorException.NotFound.class, () -> ecc.read("get-customer", "4711"));
    }

    @Test
    void connectionFailuresAreTransient() throws Exception {
        sap.fail("BAPI_SALESORDER_CREATEFROMDAT2", 1);
        var e = assertThrows(Exception.class, () -> ecc.commit("create-sales-order", "K1", json(ORDER)));
        assertFalse(e instanceof ConnectorException, e.toString());
        assertTrue(sap.orders().isEmpty());
        assertEquals("0000012000", ecc.commit("create-sales-order", "K1", json(ORDER)).path("salesOrder").asText());
    }

    @Test
    void checksTheSystem() throws Exception {
        List<CheckResult> checks = ecc.check();
        assertTrue(checks.stream().allMatch(CheckResult::ok), checks.toString());
        assertEquals("fake ECC (not a real SAP system): RFC_PING answered", checks.getFirst().detail());
        assertTrue(checks.stream().anyMatch(c -> c.name().equals("BAPI_SALESORDER_CREATEFROMDAT2")));

        try (Endpoint locked = new SapEccConnector().bind(config("{\"rfc\":{\"provider\":\"fake\"}}", "locked:pw"))) {
            List<CheckResult> bad = locked.check();
            assertFalse(bad.getFirst().ok());
            assertTrue(bad.getFirst().fix().contains("RFC user"));
        }
    }

    static JsonNode object(JsonNode cat, String name) {
        for (JsonNode o : cat.path("objects")) {
            if (o.path("name").asText().equals(name)) {
                return o;
            }
        }
        return null;
    }

    static JsonNode field(JsonNode o, String name) {
        for (JsonNode f : o.path("fields")) {
            if (f.path("name").asText().equals(name)) {
                return f;
            }
        }
        return null;
    }

    @Test
    void discoversInterfacesStructuresAndIdocs() throws Exception {
        JsonNode cat = ecc.discover(List.of("IDOC:ORDERS05", "STRUCTURE:BAPIRET2", "FUNCTION:BAPI_SALESORDER_GETSTATUS"));
        JsonNode create = object(cat, "BAPI_SALESORDER_CREATEFROMDAT2");
        assertEquals("function", create.path("kind").asText());
        assertEquals("BAPISDHD1", field(create, "ORDER_HEADER_IN").path("type").asText());
        assertTrue(field(create, "ORDER_HEADER_IN").path("required").asBoolean());
        assertFalse(field(create, "TESTRUN").path("required").asBoolean(), "optional");
        assertTrue(field(create, "SALESDOCUMENT").path("readOnly").asBoolean(), "an export");
        assertTrue(object(cat, "BAPI_SALESORDER_GETSTATUS") != null);

        JsonNode header = object(cat, "BAPISDHD1");
        assertEquals("structure", header.path("kind").asText());
        assertEquals(35, field(header, "PURCH_NO_C").path("length").asInt());
        assertEquals("CHAR", field(header, "PURCH_NO_C").path("type").asText());
        assertEquals(null, object(cat, "BAPIRET2"), "a structure the system does not have is left out");

        JsonNode e1edk01 = object(cat, "ORDERS05/E1EDK01");
        assertEquals("idoc-segment", e1edk01.path("kind").asText());
        assertEquals(35, field(e1edk01, "BELNR").path("length").asInt());

        boolean used = false;
        for (JsonNode u : cat.path("uses")) {
            used |= u.path("object").asText().equals("BAPISDHD1") && u.path("field").asText().equals("PURCH_NO_C")
                    && u.path("by").asText().equals("operation create-sales-order") && u.path("creates").asBoolean();
        }
        assertTrue(used, cat.path("uses").toString());

        // Someone shortens the field in SE11: the next discovery sees it.
        sap.structures.get("BAPISDHD1").put("PURCH_NO_C", new Object[] {"CHAR", 20, "Customer purchase order number"});
        assertEquals(20, field(object(ecc.discover(List.of()), "BAPISDHD1"), "PURCH_NO_C").path("length").asInt());

        assertThrows(Exception.class, () -> ecc.discover(List.of("FUNCTION:Z_NOPE")));
    }

    @Test
    void configuration() throws Exception {
        EccConfig c = EccConfig.from(config("""
                {"rfc":{"destination":{"ashost":"ecc","sysnr":"00","jco.client.lang":"DE"}},
                 "salesArea":{"salesOrg":"2000","distributionChannel":"20","division":"01"},"orderType":"OR","partnerRole":"SP"}""",
                "{\"user\":\"TURGON\",\"passwd\":\"s3cret\",\"client\":\"200\"}"), 0);
        assertEquals("ecc", c.destination().get("jco.client.ashost"));
        assertEquals("DE", c.destination().get("jco.client.lang"));
        assertEquals("s3cret", c.destination().get("jco.client.passwd"));
        assertEquals("200", c.destination().get("jco.client.client"));
        assertEquals("2000", c.salesOrg());
        assertEquals("OR", c.orderType());
        assertFalse(c.toString().contains("s3cret"));
        assertEquals("ecc client 200 as TURGON", c.where());

        assertThrows(ConnectorException.Invalid.class, () -> EccConfig.from(config("{\"rfc\":{\"destination\":{\"passwd\":\"x\"}}}", ""), 0));
        assertThrows(ConnectorException.Invalid.class, () -> EccConfig.from(config("{}", "no-colon"), 0));
        assertThrows(ConnectorException.Invalid.class, () -> EccConfig.from(config("{}", "{broken"), 0));
        assertThrows(ConnectorException.Invalid.class, () -> new SapEccConnector().bind(config("{\"rfc\":{\"provider\":\"nope\"}}", "u:p")));
    }
}
