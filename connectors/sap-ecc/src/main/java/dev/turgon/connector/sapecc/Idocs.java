package dev.turgon.connector.sapecc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.Event;
import dev.turgon.connector.sdk.StreamSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * IDocs to events. SAP sends an IDoc's data records as fixed-width SDATA;
 * the segment definitions (IDOCTYPE_READ_COMPLETE, read once per type) say
 * where each field is. An event's payload is the IDoc's control record and
 * its segments as a tree:
 *
 * <pre>
 * {"idoc": {"docnum": ..., "mestyp": "ORDRSP", "idoctyp": "ORDERS05", "sndprn": ...},
 *  "segments": [{"segment": "E1EDK01", "fields": {"BELNR": "0000012000", ...},
 *                "segments": [...children]}]}
 * </pre>
 */
final class Idocs implements IdocReceiver {
    private static final Logger LOG = Logger.getLogger(Idocs.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();

    /** A field's place in SDATA. */
    record Field(String name, int offset, int length) {}

    private final Rfc rfc;
    private final Map<String, EccConfig.IdocEvent> events;
    private final Map<String, StreamSink> sinks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<Field>>> definitions = new ConcurrentHashMap<>();

    Idocs(Rfc rfc, Map<String, EccConfig.IdocEvent> events) {
        this.rfc = rfc;
        this.events = events;
    }

    void open(String event, StreamSink sink) {
        sinks.put(event, sink);
    }

    void close(String event, StreamSink sink) {
        sinks.remove(event, sink);
    }

    boolean idle() {
        return sinks.isEmpty();
    }

    /** The segments of an IDoc type: their fields, from SAP's own definition. */
    Map<String, List<Field>> definition(String idoctyp, String cimtyp) throws Exception {
        String key = idoctyp + "/" + cimtyp;
        Map<String, List<Field>> d = definitions.get(key);
        if (d != null) {
            return d;
        }
        Map<String, Object> r = rfc.call("IDOCTYPE_READ_COMPLETE", Map.of("PI_IDOCTYP", idoctyp, "PI_CIMTYP", cimtyp));
        Map<String, List<Field>> raw = new HashMap<>();
        for (Map<String, String> f : Rfc.table(r, "PT_FIELDS")) {
            raw.computeIfAbsent(f.get("SEGMENTTYP").trim(), k -> new ArrayList<>()).add(new Field(f.get("FIELDNAME").trim(),
                    Integer.parseInt(f.get("BYTE_FIRST").trim()), Integer.parseInt(f.get("EXTLEN").trim())));
        }
        // Offsets count from the data record's start (SDATA follows a 63-byte
        // header); taking each segment's first field as SDATA's start reads
        // either convention.
        d = new HashMap<>();
        for (var e : raw.entrySet()) {
            int base = e.getValue().stream().mapToInt(Field::offset).min().orElse(0);
            d.put(e.getKey(), e.getValue().stream().map(f -> new Field(f.name(), f.offset() - base, f.length())).toList());
        }
        if (d.isEmpty()) {
            throw new IllegalStateException("SAP describes no segments for IDoc type " + key);
        }
        definitions.put(key, d);
        return d;
    }

    /** An IDoc as an event. */
    Event event(String name, Map<String, String> control, List<Map<String, String>> data) throws Exception {
        String docnum = control.getOrDefault("DOCNUM", "").trim();
        String idoctyp = control.getOrDefault("IDOCTYP", "").trim();
        Map<String, List<Field>> def = data.isEmpty() ? Map.of() : definition(idoctyp, control.getOrDefault("CIMTYP", "").trim());
        ObjectNode payload = JSON.createObjectNode();
        ObjectNode head = payload.putObject("idoc");
        for (String k : List.of("DOCNUM", "IDOCTYP", "CIMTYP", "MESTYP", "MESCOD", "MESFCT", "SNDPRT", "SNDPRN", "RCVPRT", "RCVPRN", "CREDAT", "CRETIM")) {
            String v = control.getOrDefault(k, "").trim();
            if (!v.isEmpty()) {
                head.put(k.toLowerCase(), v);
            }
        }
        ArrayNode top = payload.putArray("segments");
        Map<String, ArrayNode> children = new LinkedHashMap<>();
        for (Map<String, String> rec : data) {
            String seg = rec.getOrDefault("SEGNAM", "").trim();
            ObjectNode node = JSON.createObjectNode().put("segment", seg);
            ObjectNode fields = node.putObject("fields");
            String sdata = rec.getOrDefault("SDATA", "");
            for (Field f : def.getOrDefault(seg, List.of())) {
                if (f.offset() >= sdata.length()) {
                    continue;
                }
                String v = sdata.substring(f.offset(), Math.min(sdata.length(), f.offset() + f.length())).trim();
                if (!v.isEmpty()) {
                    fields.put(f.name(), v);
                }
            }
            ArrayNode kids = node.putArray("segments");
            children.put(rec.getOrDefault("SEGNUM", "").trim(), kids);
            String parent = rec.getOrDefault("PSGNUM", "").trim().replaceFirst("^0+", "");
            ArrayNode into = parent.isEmpty() ? top : children.entrySet().stream()
                    .filter(c -> c.getKey().replaceFirst("^0+", "").equals(parent)).map(Map.Entry::getValue).findFirst().orElse(top);
            into.add(node);
        }
        String sender = control.getOrDefault("SNDPRN", "").trim();
        return new Event((sender.isEmpty() ? "" : sender + "/") + docnum, 0, name, payload);
    }

    /**
     * One tRFC transaction: its IDocs go to the streams of their events and
     * return only once the worker stored them. IDocs no event takes are
     * accepted and dropped (logged); IDocs of an event with no open stream
     * fail the transaction, so SAP sends them again, to a worker that listens.
     */
    @Override
    public void receive(String tid, List<Map<String, String>> control, List<Map<String, String>> data) throws Exception {
        Map<String, List<Map<String, String>>> byDoc = new HashMap<>();
        for (Map<String, String> d : data) {
            byDoc.computeIfAbsent(d.getOrDefault("DOCNUM", "").trim(), k -> new ArrayList<>()).add(d);
        }
        Map<String, List<Map<String, String>>> matched = new LinkedHashMap<>();
        int ignored = 0;
        for (Map<String, String> c : control) {
            String mestyp = c.getOrDefault("MESTYP", "").trim(), idoctyp = c.getOrDefault("IDOCTYP", "").trim();
            boolean taken = false;
            for (var e : events.entrySet()) {
                if (e.getValue().matches(mestyp, idoctyp)) {
                    matched.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(c);
                    taken = true;
                }
            }
            if (!taken) {
                ignored++;
            }
        }
        for (String event : matched.keySet()) {
            StreamSink sink = sinks.get(event);
            if (sink == null || !sink.open()) {
                LOG.warning(() -> "transaction " + tid + " refused: no worker is listening for " + event + "; SAP sends it again (SM58)");
                throw new IllegalStateException("no worker is listening for " + event + "; SAP will send the IDocs again");
            }
        }
        Map<String, List<Event>> out = new LinkedHashMap<>();
        for (var m : matched.entrySet()) {
            for (Map<String, String> c : m.getValue()) {
                out.computeIfAbsent(m.getKey(), k -> new ArrayList<>())
                        .add(event(m.getKey(), c, byDoc.getOrDefault(c.getOrDefault("DOCNUM", "").trim(), List.of())));
            }
        }
        if (ignored > 0) {
            int n = ignored;
            LOG.info(() -> "transaction " + tid + ": " + n + " IDoc(s) of message types no event takes, accepted and dropped");
        }
        for (var e : out.entrySet()) {
            StreamSink sink = sinks.get(e.getKey());
            if (sink == null) {
                throw new IllegalStateException("no worker is listening for " + e.getKey() + "; SAP will send the IDocs again");
            }
            sink.deliver(e.getValue(), null);
        }
    }
}
