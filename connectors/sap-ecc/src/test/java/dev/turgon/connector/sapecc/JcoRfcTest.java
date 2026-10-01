package dev.turgon.connector.sapecc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.ext.Environment;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** The reflective JCo adapter, against the JCo stand-in in this test tree. */
class JcoRfcTest {

    @Test
    void drivesJcoByReflection() throws Exception {
        FakeEcc.reset();
        assertTrue(new JcoProvider().available());
        Endpoint ecc = new SapEccConnector().bind(EccEndpointTest.config(
                "{\"rfc\":{\"provider\":\"jco\",\"destination\":{\"ashost\":\"jco-ecc\",\"sysnr\":\"00\",\"client\":\"100\"}}}", "TURGON:s3cret"));
        String name = JCoDestinationManager.SEEN.keySet().stream().reduce((a, b) -> b).orElseThrow();
        assertTrue(name.startsWith("turgon-sap-ecc-"), name);
        Properties p = JCoDestinationManager.SEEN.get(name);
        assertEquals("TURGON", p.getProperty("jco.client.user"));
        assertEquals("s3cret", p.getProperty("jco.client.passwd"));
        assertEquals("100", p.getProperty("jco.client.client"));
        assertEquals("3", p.getProperty("jco.destination.pool_capacity"));

        JsonNode order = EccEndpointTest.json(EccEndpointTest.ORDER);
        assertTrue(ecc.simulate("create-sales-order", order).path("testRun").asBoolean());
        JsonNode created = ecc.commit("create-sales-order", "006A", order);
        assertEquals("0000012000", created.path("salesOrder").asText());
        FakeEcc sap = FakeEcc.system("jco-ecc");
        assertEquals("M-8", sap.orders().get("0000012000").items().get(1).get("MATERIAL"));
        ecc.confirm("create-sales-order", created);
        assertTrue(ecc.commit("create-sales-order", "006A", order).path("existing").asBoolean());
        assertEquals("Ada Lovelace GmbH", ecc.read("get-customer", "1000").path("name").asText());
        var refused = assertThrows(ConnectorException.Rejected.class,
                () -> ecc.commit("create-sales-order", "K9", EccEndpointTest.json(EccEndpointTest.ORDER.replace("\"1000\"", "\"1002\""))));
        assertTrue(refused.getMessage().contains("blocked for sales"));
        assertTrue(ecc.commit("cancel-sales-order", "006A#compensate", created).path("deleted").asBoolean());
        assertTrue(sap.orders().isEmpty());
        assertTrue(ecc.check().stream().allMatch(CheckResult::ok), ecc.check().toString());

        // ABAP exceptions become AbapError; other JCo failures pass through.
        Rfc rfc = new JcoProvider().open(EccEndpointTest.config("{}", "u:p"),
                EccConfig.from(EccEndpointTest.config("{\"rfc\":{\"destination\":{\"ashost\":\"jco-ecc\"}}}", "u:p"), 0));
        var abap = assertThrows(Rfc.AbapError.class, () -> rfc.call("Z_RAISE", Map.of()));
        assertEquals("NOT_AUTHORIZED", abap.key());
        assertThrows(IllegalStateException.class, () -> rfc.call("Z_MISSING", Map.of()));
        assertThrows(IllegalArgumentException.class, () -> rfc.call("RFC_PING", Map.of("NOPE", "x")));
        assertFalse(rfc.exists("Z_MISSING"));
        rfc.close();

        ecc.close();
        assertTrue(Environment.DELETED.contains(name), Environment.DELETED.toString());
    }
}
