package dev.turgon.connector.sapecc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Event;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Events from change pointers (BDCP2), polled. SAP writes a change pointer
 * for every changed field of an object whose message type is active (BD50,
 * with the fields in BD52). Turgon reads them with RFC_READ_TABLE and never
 * marks them processed, so its message type should be its own (a copy of
 * DEBMAS, say): the cursor is the pointer's number (CPIDENT).
 *
 * <p>RFC_READ_TABLE does not sort and cuts at ROWCOUNT, so a capped read
 * may miss lower numbers. A read that comes back full is narrowed to the
 * median number it saw until the range fits; only a range read in full
 * moves the cursor. Pointers younger than {@code settleSeconds} wait, so a
 * number drawn before a slower transaction commits is not passed over.
 *
 * <p>An event is the pointers of one change document (CDOBJCL, CDOBJID,
 * CDCHGNO) read together:
 *
 * <pre>
 * {"messageType": "ZTURGON_DEBMAS", "objectClass": "DEBI", "objectId": "0000001000",
 *  "changeNumber": "0000000042", "changedAt": "2026-10-29T12:00:00Z", "user": "JDOE",
 *  "changes": [{"table": "KNA1", "key": "1000000001000", "field": "NAME1", "kind": "U"}],
 *  "record": {...the object as get-customer reads it, null once deleted}}
 * </pre>
 */
final class ChangePointers {
    static final String TABLE = "BDCP2";
    static final List<String> FIELDS = List.of("CPIDENT", "TABNAME", "TABKEY", "FLDNAME", "CRETIME", "USRNAME", "CDOBJCL",
            "CDOBJID", "CDCHGNO", "CDCHGID");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /** Reads one record for an event's "record" (a read operation such as get-customer). */
    @FunctionalInterface
    interface Reader {
        JsonNode read(String operation, String id) throws Exception;
    }

    record Pointer(long number, String table, String key, String field, Instant created, String user, String objectClass,
            String objectId, String changeNumber, String kind) {}

    private final Rfc rfc;
    private final EccConfig.ChangePointerConfig cfg;
    private final Supplier<Instant> clock;

    ChangePointers(Rfc rfc, EccConfig.ChangePointerConfig cfg, Supplier<Instant> clock) {
        this.rfc = rfc;
        this.cfg = cfg;
        this.clock = clock;
    }

    List<Event> poll(String event, long after, int limit, Reader reader) throws Exception {
        EccConfig.ChangePointerEvent ev = cfg.events().get(event);
        if (ev == null) {
            throw new ConnectorException.Invalid("event " + event + " is not configured under changePointers.events");
        }
        List<Pointer> ps = complete(ev.messageType(), after);
        Instant settled = clock.get().minusSeconds(cfg.settleSeconds());
        List<Event> out = new ArrayList<>();
        int i = 0;
        while (i < ps.size() && out.size() < limit) {
            Pointer first = ps.get(i);
            if (first.created().isAfter(settled)) {
                break; // a lower number may still commit: wait for it
            }
            int j = i + 1;
            while (j < ps.size() && same(first, ps.get(j)) && !ps.get(j).created().isAfter(settled)) {
                j++;
            }
            out.add(event(event, ev, ps.subList(i, j), reader));
            i = j;
        }
        return out;
    }

    private static boolean same(Pointer a, Pointer b) {
        return a.objectClass().equals(b.objectClass()) && a.objectId().equals(b.objectId()) && a.changeNumber().equals(b.changeNumber());
    }

    private Event event(String name, EccConfig.ChangePointerEvent ev, List<Pointer> group, Reader reader) throws Exception {
        Pointer first = group.getFirst(), last = group.getLast();
        ObjectNode p = JSON.createObjectNode();
        p.put("messageType", ev.messageType());
        p.put("objectClass", first.objectClass());
        p.put("objectId", first.objectId());
        p.put("changeNumber", first.changeNumber());
        p.put("changedAt", first.created().toString());
        p.put("user", first.user());
        ArrayNode changes = p.putArray("changes");
        for (Pointer c : group) {
            changes.addObject().put("table", c.table()).put("key", c.key()).put("field", c.field()).put("kind", c.kind());
        }
        if (!ev.record().isEmpty()) {
            try {
                p.set("record", reader.read(ev.record(), first.objectId()));
            } catch (ConnectorException.NotFound e) {
                p.putNull("record"); // deleted since: the pointers still say what changed
            }
        }
        return new Event(TABLE + ":" + ev.messageType() + ":" + last.number(), last.number(), name, p);
    }

    /** The pointers above after, in order, up to a number below which none is missing. */
    List<Pointer> complete(String messageType, long after) throws Exception {
        Long below = null;
        while (true) {
            List<Pointer> got = read(messageType, after, below, cfg.pageRows() + 1);
            if (got.size() <= cfg.pageRows()) {
                got.sort(Comparator.comparingLong(Pointer::number));
                return got;
            }
            got.sort(Comparator.comparingLong(Pointer::number));
            long median = got.get(got.size() / 2 - 1).number();
            if (below != null && median >= below) {
                throw new IllegalStateException("RFC_READ_TABLE returned pointers outside the range it was asked for");
            }
            below = median;
        }
    }

    private List<Pointer> read(String messageType, long after, Long upTo, int rows) throws Exception {
        List<Map<String, String>> options = new ArrayList<>();
        options.add(Map.of("TEXT", "MESTYPE = '" + messageType + "'"));
        options.add(Map.of("TEXT", "AND CPIDENT > '" + cpident(after) + "'"));
        if (upTo != null) {
            options.add(Map.of("TEXT", "AND CPIDENT <= '" + cpident(upTo) + "'"));
        }
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("QUERY_TABLE", TABLE);
        params.put("DELIMITER", "");
        params.put("ROWCOUNT", Integer.toString(rows));
        params.put("OPTIONS", options);
        params.put("FIELDS", FIELDS.stream().map(f -> Map.of("FIELDNAME", f)).toList());
        Map<String, Object> r = rfc.call("RFC_READ_TABLE", params);
        Map<String, int[]> at = new LinkedHashMap<>();
        for (Map<String, String> f : Rfc.table(r, "FIELDS")) {
            at.put(f.get("FIELDNAME").trim(), new int[] {Integer.parseInt(f.get("OFFSET").trim()), Integer.parseInt(f.get("LENGTH").trim())});
        }
        List<Pointer> out = new ArrayList<>();
        for (Map<String, String> row : Rfc.table(r, "DATA")) {
            String wa = row.getOrDefault("WA", "");
            long number;
            try {
                number = Long.parseLong(field(wa, at, "CPIDENT"));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("BDCP2-CPIDENT is not a number: " + field(wa, at, "CPIDENT"));
            }
            out.add(new Pointer(number, field(wa, at, "TABNAME"), field(wa, at, "TABKEY"), field(wa, at, "FLDNAME"),
                    created(field(wa, at, "CRETIME")), field(wa, at, "USRNAME"), field(wa, at, "CDOBJCL"), field(wa, at, "CDOBJID"),
                    field(wa, at, "CDCHGNO"), field(wa, at, "CDCHGID")));
        }
        return out;
    }

    /** CPIDENT is a 20-character number, zero-padded, so it compares as text. */
    static String cpident(long n) {
        return String.format("%020d", n);
    }

    private static String field(String wa, Map<String, int[]> at, String name) {
        int[] p = at.get(name);
        if (p == null) {
            throw new IllegalStateException("RFC_READ_TABLE returned no field " + name + " of " + TABLE);
        }
        if (p[0] >= wa.length()) {
            return "";
        }
        return wa.substring(p[0], Math.min(wa.length(), p[0] + p[1])).trim();
    }

    /** CRETIME is a UTC time stamp, YYYYMMDDhhmmss (a packed number, so possibly with a fraction). */
    static Instant created(String s) {
        String digits = s.replace(",", ".").split("\\.")[0].trim();
        if (digits.length() < 14) {
            digits = String.format("%14s", digits).replace(' ', '0');
        }
        return LocalDateTime.parse(digits.substring(0, 14), STAMP).toInstant(ZoneOffset.UTC);
    }
}
