package dev.turgon.connector.sapecc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import dev.turgon.connector.sdk.Event;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChangePointersTest {
    static final ObjectMapper JSON = new ObjectMapper();
    FakeEcc sap;

    @BeforeEach
    void reset() {
        FakeEcc.reset();
        sap = FakeEcc.system("ecc1");
    }

    static Endpoint bind(String changePointers) throws Exception {
        String cfg = "{\"rfc\":{\"provider\":\"fake\",\"destination\":{\"ashost\":\"ecc1\",\"client\":\"100\"}},\"changePointers\":" + changePointers + "}";
        return new SapEccConnector().bind(new EndpointConfig("sap-ecc", "sap-ecc", "0.5.0", JSON.readTree(cfg), "TURGON:pw", Map.of()));
    }

    @Test
    void customerChangesArePolledInOrderWithTheCustomer() throws Exception {
        sap.changeCustomer("0000001000", "Ada Lovelace AG", "JDOE");
        sap.changeCustomer("0000001001", "Hopper Labs Inc", "JDOE");
        sap.deleteCustomer("0000001001", "JDOE");
        try (Endpoint e = bind("{\"settleSeconds\":0,\"events\":{\"Customer.Changed\":{\"messageType\":\"zturgon_debmas\",\"record\":\"get-customer\"}}}")) {
            List<Event> evs = e.poll("Customer.Changed", 0, 10);
            assertEquals(3, evs.size());
            JsonNode first = evs.getFirst().payload();
            assertEquals("0000001000", first.path("objectId").asText());
            assertEquals("DEBI", first.path("objectClass").asText());
            assertEquals("JDOE", first.path("user").asText());
            assertEquals("NAME1", first.path("changes").get(0).path("field").asText());
            assertEquals("U", first.path("changes").get(0).path("kind").asText());
            assertEquals("Ada Lovelace AG", first.path("record").path("name").asText());
            assertTrue(evs.get(2).payload().path("record").isNull(), "a deleted customer has no record");
            assertEquals("D", evs.get(2).payload().path("changes").get(0).path("kind").asText());
            assertTrue(evs.get(0).position() < evs.get(1).position() && evs.get(1).position() < evs.get(2).position());
            assertEquals("BDCP2:ZTURGON_DEBMAS:" + evs.getFirst().position(), evs.getFirst().id());

            assertEquals(List.of(), e.poll("Customer.Changed", evs.get(2).position(), 10));
            List<Event> two = e.poll("Customer.Changed", evs.getFirst().position(), 1);
            assertEquals(List.of(evs.get(1).id()), two.stream().map(Event::id).toList());
            // Orders are another object class: their pointers are not customer events.
            assertThrows(ConnectorException.Invalid.class, () -> e.poll("SalesOrder.Changed", 0, 10));
        }
    }

    @Test
    void aFullReadIsNarrowedSoNoPointerIsSkipped() throws Exception {
        for (int i = 0; i < 25; i++) {
            sap.changeCustomer("0000001000", "Ada " + i, "JDOE");
        }
        // The fake returns the newest rows first: a capped read misses the oldest.
        try (Endpoint e = bind("{\"pageRows\":4,\"settleSeconds\":0,\"events\":{\"Customer.Changed\":{\"messageType\":\"ZTURGON_DEBMAS\"}}}")) {
            List<Long> seen = new ArrayList<>();
            long cursor = 0;
            for (int round = 0; round < 50 && seen.size() < 25; round++) {
                for (Event ev : e.poll("Customer.Changed", cursor, 3)) {
                    seen.add(ev.position());
                    cursor = ev.position();
                }
            }
            assertEquals(25, seen.size());
            for (int i = 1; i < seen.size(); i++) {
                assertEquals(seen.get(i - 1) + 1, seen.get(i), "in order, none skipped");
            }
            assertTrue(e.poll("Customer.Changed", cursor, 3).isEmpty());
        }
    }

    @Test
    void youngPointersWaitForSlowerTransactions() throws Exception {
        sap.changeCustomer("0000001000", "Ada Lovelace AG", "JDOE");
        sap.changeCustomer("0000001001", "Hopper Labs Inc", "JDOE");
        var cfg = new EccConfig.ChangePointerConfig(100, 60,
                Map.of("Customer.Changed", new EccConfig.ChangePointerEvent("ZTURGON_DEBMAS", "")));
        AtomicReference<Instant> now = new AtomicReference<>(Instant.now());
        Rfc rfc = new dev.turgon.connector.sapecc.fake.FakeRfcProvider().open(null, EccConfig.from(
                new EndpointConfig("s", "sap-ecc", "0.5.0", JSON.readTree("{\"rfc\":{\"destination\":{\"ashost\":\"ecc1\"}}}"), "U:pw", Map.of()), 0));
        ChangePointers cp = new ChangePointers(rfc, cfg, now::get);
        assertTrue(cp.poll("Customer.Changed", 0, 10, (op, id) -> null).isEmpty());
        now.set(now.get().plusSeconds(61));
        assertEquals(2, cp.poll("Customer.Changed", 0, 10, (op, id) -> null).size());
        // A young pointer holds back itself and every later one, settled or not.
        sap.changeCustomer("0000001000", "Ada again", "JDOE");
        sap.changePointers.getLast().put("CRETIME", "20991231000000");
        sap.changeCustomer("0000001001", "Hopper again", "JDOE");
        assertEquals(2, cp.poll("Customer.Changed", 0, 10, (op, id) -> null).size());
    }

    @Test
    void ordersAndTheirDeletionArePolledToo() throws Exception {
        try (Endpoint e = bind("{\"settleSeconds\":0,\"events\":{\"SalesOrder.Changed\":{\"messageType\":\"ZTURGON_ORDERS\"}}}")) {
            JsonNode created = e.commit("create-sales-order", "PO-1", JSON.readTree(EccEndpointTest.ORDER));
            e.commit("cancel-sales-order", "PO-1#compensate", created);
            List<Event> evs = e.poll("SalesOrder.Changed", 0, 10);
            assertEquals(List.of("I", "D"), evs.stream().map(ev -> ev.payload().path("changes").get(0).path("kind").asText()).toList());
            assertEquals(created.path("salesOrder").asText(), evs.getFirst().payload().path("objectId").asText());
        }
    }

    @Test
    void checkReadsTheTableAndConfigurationIsValidated() throws Exception {
        try (Endpoint e = bind("{\"events\":{\"Customer.Changed\":{\"messageType\":\"ZTURGON_DEBMAS\"}}}")) {
            CheckResult cp = e.check().stream().filter(c -> c.name().equals("change pointers")).findFirst().orElseThrow();
            assertTrue(cp.ok(), cp.detail());
            assertTrue(cp.detail().contains("BD50"), cp.detail());
        }
        sap.structures.get("BDCP2").remove("CDCHGNO");
        try (Endpoint e = bind("{\"events\":{\"Customer.Changed\":{\"messageType\":\"ZTURGON_DEBMAS\"}}}")) {
            CheckResult cp = e.check().stream().filter(c -> c.name().equals("change pointers")).findFirst().orElseThrow();
            assertTrue(!cp.ok() && cp.detail().contains("CDCHGNO"), cp.detail());
        }
        for (String bad : List.of("{\"events\":{\"X\":{\"messageType\":\"\"}}}", "{\"pageRows\":0}", "{\"settleSeconds\":-1}",
                "{\"events\":{\"X\":{\"messageType\":\"DEBMAS\",\"record\":\"get-order\"}}}")) {
            assertThrows(ConnectorException.Invalid.class, () -> bind(bad), bad);
        }
        assertEquals("20261029120000", ChangePointers.cpident(20261029120000L).substring(6));
        assertEquals(Instant.parse("2026-10-29T12:00:00Z"), ChangePointers.created("20261029120000.5"));
    }
}
