package dev.turgon.connector.sapecc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.Event;
import dev.turgon.connector.sdk.StreamSink;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class IdocTest {
    static final String CONFIG = """
            {"rfc":{"provider":"fake","destination":{"ashost":"ecc-idoc"}},
             "idoc":{"server":{"gwhost":"ecc-idoc","gwserv":"sapgw00","progid":"TURGON_IDOC"},
                     "events":{"SalesOrder.Created":{"messageTypes":["ORDRSP"],"idocTypes":["ORDERS05"]}}}}""";

    /** A worker's end: stores what arrives, or fails while told to. */
    static final class Worker implements StreamSink {
        final List<Event> got = new CopyOnWriteArrayList<>();
        final AtomicBoolean failing = new AtomicBoolean();

        @Override
        public void deliver(List<Event> events, byte[] resume) {
            if (failing.get()) {
                throw new IllegalStateException("the inbox is away");
            }
            got.addAll(events);
        }

        @Override
        public boolean open() {
            return true;
        }
    }

    static void await(java.util.function.BooleanSupplier ok) throws InterruptedException {
        for (int i = 0; i < 200 && !ok.getAsBoolean(); i++) {
            Thread.sleep(25);
        }
        assertTrue(ok.getAsBoolean(), "timed out");
    }

    static JsonNode segment(JsonNode segments, String name) {
        for (JsonNode s : segments) {
            if (s.path("segment").asText().equals(name)) {
                return s;
            }
        }
        return null;
    }

    @Test
    void orderConfirmationsBecomeEvents() throws Exception {
        FakeEcc.reset();
        try (Endpoint ecc = new SapEccConnector().bind(EccEndpointTest.config(CONFIG, "TURGON:pw"))) {
            assertEquals(Set.of("SalesOrder.Created"), ecc.streams());
            FakeEcc sap = FakeEcc.system("ecc-idoc");

            // Saved before any worker listens: SAP keeps the IDoc (SM58).
            String first = ecc.commit("create-sales-order", "006A", EccEndpointTest.json(EccEndpointTest.ORDER)).path("salesOrder").asText();
            assertEquals(1, sap.pending().size()); // queued until a server program registers
            Worker w = new Worker();
            AutoCloseable sub = ecc.stream("SalesOrder.Created", new byte[0], w);
            await(() -> w.got.size() == 1);

            Event e = w.got.getFirst();
            assertEquals("ECCCLNT100/0000000000004711", e.id());
            assertEquals("SalesOrder.Created", e.name());
            JsonNode p = e.payload();
            assertEquals("ORDRSP", p.path("idoc").path("mestyp").asText());
            assertEquals("ORDERS05", p.path("idoc").path("idoctyp").asText());
            assertEquals(first, segment(p.path("segments"), "E1EDK01").path("fields").path("BELNR").asText());
            assertEquals("EUR", segment(p.path("segments"), "E1EDK01").path("fields").path("CURCY").asText());
            JsonNode party = segment(p.path("segments"), "E1EDKA1").path("fields");
            assertEquals("0000001000", party.path("PARTN").asText());
            assertEquals("Ada Lovelace GmbH", party.path("NAME1").asText());
            assertEquals("006A", segment(p.path("segments"), "E1EDK02").path("fields").path("BELNR").asText());
            JsonNode item = segment(p.path("segments"), "E1EDP01");
            assertEquals("2", item.path("fields").path("MENGE").asText());
            assertEquals("M-7", item.path("segments").get(0).path("fields").path("IDTNR").asText(), item.toString());

            // The worker cannot store: the transaction fails and SAP sends it again.
            w.failing.set(true);
            ecc.commit("create-sales-order", "006B", EccEndpointTest.json(EccEndpointTest.ORDER));
            await(() -> !sap.pending().isEmpty() && sap.pending().getFirst().attempts >= 2);
            assertTrue(sap.pending().getFirst().lastError.contains("inbox is away"));
            w.failing.set(false);
            await(() -> w.got.size() == 2 && sap.pending().isEmpty());

            // IDocs no event takes are accepted and dropped.
            sap.send(List.of(Map.of("DOCNUM", "0000000000009999", "IDOCTYP", "MATMAS05", "MESTYP", "MATMAS")), List.of());
            await(() -> sap.sent.stream().anyMatch(t -> t.control.getFirst().get("MESTYP").equals("MATMAS")));
            assertEquals(2, w.got.size());

            // No worker listens: the program is unregistered and SAP keeps the IDoc.
            sub.close();
            ecc.commit("create-sales-order", "006C", EccEndpointTest.json(EccEndpointTest.ORDER));
            await(() -> !sap.pending().isEmpty() && sap.pending().getFirst().attempts >= 1);
            assertTrue(sap.pending().getFirst().lastError.contains("no server program"), sap.pending().getFirst().lastError);
            Worker again = new Worker();
            try (AutoCloseable sub2 = ecc.stream("SalesOrder.Created", null, again)) {
                await(() -> again.got.size() == 1);
            }
            assertTrue(ecc.check().stream().anyMatch(c -> c.name().equals("idoc") && c.detail().contains("TURGON_IDOC")));
        }
    }

    @Test
    void anOpenEventWithoutAWorkerFailsTheTransaction() throws Exception {
        FakeEcc.reset();
        String two = CONFIG.replace("\"events\":{", "\"events\":{\"Material.Changed\":{\"messageTypes\":[\"MATMAS\"]},");
        try (Endpoint ecc = new SapEccConnector().bind(EccEndpointTest.config(two, "TURGON:pw"))) {
            Worker w = new Worker();
            try (AutoCloseable sub = ecc.stream("SalesOrder.Created", null, w)) {
                FakeEcc sap = FakeEcc.system("ecc-idoc");
                sap.send(List.of(Map.of("DOCNUM", "0000000000009999", "IDOCTYP", "MATMAS05", "MESTYP", "MATMAS")), List.of());
                await(() -> !sap.pending().isEmpty() && sap.pending().getFirst().attempts >= 1);
                assertTrue(sap.pending().getFirst().lastError.contains("no worker is listening for Material.Changed"));
            }
        }
    }

    @Test
    void configuration() throws Exception {
        assertThrows(ConnectorException.Invalid.class, () -> EccConfig.from(EccEndpointTest.config(
                "{\"idoc\":{\"events\":{\"X\":{\"messageTypes\":[\"ORDRSP\"]}}}}", "u:p"), 0));
        assertThrows(ConnectorException.Invalid.class, () -> EccConfig.from(EccEndpointTest.config(
                "{\"idoc\":{\"server\":{\"gwhost\":\"h\",\"gwserv\":\"s\",\"progid\":\"P\"},\"events\":{\"X\":{}}}}", "u:p"), 0));
        EccConfig c = EccConfig.from(EccEndpointTest.config(CONFIG.replace("\"progid\"", "\"connectionCount\":4,\"progid\""), "u:p"), 0);
        assertEquals("4", c.server().get("jco.server.connection_count"));
        assertEquals("TURGON_IDOC", c.server().get("jco.server.progid"));
        assertTrue(c.idocEvents().get("SalesOrder.Created").matches("ORDRSP", "ORDERS05"));
        assertTrue(!c.idocEvents().get("SalesOrder.Created").matches("ORDRSP", "ORDERS01"));
    }
}
