package dev.turgon.connector.sapecc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.EndpointConfig;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An endpoint's configuration (the connection's {@code config}), all optional:
 *
 * <pre>
 * rfc:
 *   provider: jco                # default: the one available
 *   destination:                 # JCo client properties; "ashost" means jco.client.ashost
 *     ashost: ecc.example.com
 *     sysnr: "00"
 *     client: "100"
 *     lang: EN
 * salesArea: { salesOrg: "1000", distributionChannel: "10", division: "00" }
 * orderType: TA                  # TA is the standard order (OR with English keys)
 * partnerRole: AG                # the sold-to party (SP with English keys)
 * lookupByPurchaseOrder: true    # find a retried create by its purchase order number
 * idoc:                          # events: IDocs SAP sends to a registered server program
 *   server: { gwhost: ecc.example.com, gwserv: sapgw00, progid: TURGON_IDOC, connectionCount: 2 }
 *   events:
 *     SalesOrder.Created: { messageTypes: [ORDRSP], idocTypes: [ORDERS05] }
 * changePointers:                # events: change pointers (BDCP2), polled
 *   pageRows: 1000               # rows per RFC_READ_TABLE call
 *   settleSeconds: 60            # how long a pointer waits before it is read
 *   events:
 *     Customer.Changed: { messageType: ZTURGON_DEBMAS, record: get-customer }
 * </pre>
 *
 * The secret is the RFC user: {@code user:password}, or a JSON object of
 * destination properties ({@code {"user": ..., "passwd": ..., "ashost": ...}}).
 * Passwords are refused in the configuration, which is not secret.
 */
public record EccConfig(String provider, Map<String, String> destination, String salesOrg, String distributionChannel,
        String division, String orderType, String partnerRole, boolean lookupByPurchaseOrder, int peakLimit,
        Map<String, String> server, Map<String, IdocEvent> idocEvents, ChangePointerConfig changePointers) {

    /** Polled change pointer events: each names its message type, and optionally a read that fills "record". */
    public record ChangePointerConfig(int pageRows, long settleSeconds, Map<String, ChangePointerEvent> events) {}

    public record ChangePointerEvent(String messageType, String record) {}

    /** The read operations an event's record may come from. */
    static final java.util.Set<String> RECORD_READS = java.util.Set.of("get-customer");

    /** Which IDocs make an event: their message types, and optionally their basic types. */
    public record IdocEvent(java.util.Set<String> messageTypes, java.util.Set<String> idocTypes) {
        public boolean matches(String mestyp, String idoctyp) {
            return messageTypes.contains(mestyp) && (idocTypes.isEmpty() || idocTypes.contains(idoctyp));
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    public static EccConfig from(EndpointConfig e, int maxConcurrentCalls) throws ConnectorException.Invalid {
        JsonNode c = e.config();
        Map<String, String> dest = new TreeMap<>();
        c.path("rfc").path("destination").properties().forEach(p -> dest.put(property(p.getKey()), p.getValue().asText()));
        for (String k : dest.keySet()) {
            if (k.endsWith(".passwd") || k.endsWith(".password")) {
                throw new ConnectorException.Invalid("rfc.destination must not hold the password: put it in the secret");
            }
        }
        String secret = e.secret() == null ? "" : e.secret().trim();
        if (secret.startsWith("{")) {
            try {
                JSON.readTree(secret).properties().forEach(p -> dest.put(property(p.getKey()), p.getValue().asText()));
            } catch (Exception x) {
                throw new ConnectorException.Invalid("the secret is neither user:password nor a JSON object");
            }
        } else if (!secret.isEmpty()) {
            int i = secret.indexOf(':');
            if (i <= 0) {
                throw new ConnectorException.Invalid("the secret is neither user:password nor a JSON object");
            }
            dest.put("jco.client.user", secret.substring(0, i));
            dest.put("jco.client.passwd", secret.substring(i + 1));
        }
        int peak = maxConcurrentCalls > 0 ? maxConcurrentCalls : 4;
        dest.putIfAbsent("jco.destination.peak_limit", Integer.toString(peak));
        dest.putIfAbsent("jco.destination.pool_capacity", Integer.toString(Math.min(peak, 3)));
        JsonNode area = c.path("salesArea");
        Map<String, String> server = new TreeMap<>();
        c.path("idoc").path("server").properties().forEach(p -> {
            String k = p.getKey().equals("connectionCount") ? "connection_count" : p.getKey();
            server.put(k.contains(".") ? k : "jco.server." + k, p.getValue().asText());
        });
        Map<String, IdocEvent> events = new TreeMap<>();
        for (var ev : c.path("idoc").path("events").properties()) {
            java.util.Set<String> mt = new java.util.TreeSet<>(), it = new java.util.TreeSet<>();
            ev.getValue().path("messageTypes").forEach(v -> mt.add(v.asText().toUpperCase()));
            ev.getValue().path("idocTypes").forEach(v -> it.add(v.asText().toUpperCase()));
            if (mt.isEmpty()) {
                throw new ConnectorException.Invalid("idoc.events." + ev.getKey() + ".messageTypes must name at least one message type");
            }
            events.put(ev.getKey(), new IdocEvent(mt, it));
        }
        if (!events.isEmpty()) {
            for (String k : List.of("jco.server.gwhost", "jco.server.gwserv", "jco.server.progid")) {
                if (!server.containsKey(k)) {
                    throw new ConnectorException.Invalid("idoc.server." + k.substring("jco.server.".length()) + " is required to receive IDocs");
                }
            }
            server.putIfAbsent("jco.server.connection_count", "2");
        }
        JsonNode cp = c.path("changePointers");
        Map<String, ChangePointerEvent> cpEvents = new TreeMap<>();
        for (var ev : cp.path("events").properties()) {
            String mt = ev.getValue().path("messageType").asText("").trim().toUpperCase();
            if (!mt.matches("[A-Z0-9_/]{1,30}")) {
                throw new ConnectorException.Invalid("changePointers.events." + ev.getKey() + ".messageType must be an SAP message type (BD50)");
            }
            if (events.containsKey(ev.getKey())) {
                throw new ConnectorException.Invalid("event " + ev.getKey() + " is configured both under idoc.events and changePointers.events");
            }
            String record = ev.getValue().path("record").asText("");
            if (!record.isEmpty() && !RECORD_READS.contains(record)) {
                throw new ConnectorException.Invalid("changePointers.events." + ev.getKey() + ".record must be one of " + RECORD_READS);
            }
            cpEvents.put(ev.getKey(), new ChangePointerEvent(mt, record));
        }
        int pageRows = cp.path("pageRows").asInt(1000);
        if (pageRows < 1 || pageRows > 10000) {
            throw new ConnectorException.Invalid("changePointers.pageRows must be between 1 and 10000");
        }
        long settle = cp.path("settleSeconds").asLong(60);
        if (settle < 0 || settle > 3600) {
            throw new ConnectorException.Invalid("changePointers.settleSeconds must be between 0 and 3600");
        }
        return new EccConfig(c.path("rfc").path("provider").asText(""), new LinkedHashMap<>(dest),
                area.path("salesOrg").asText("1000"), area.path("distributionChannel").asText("10"),
                area.path("division").asText("00"), c.path("orderType").asText("TA"), c.path("partnerRole").asText("AG"),
                c.path("lookupByPurchaseOrder").asBoolean(true), peak, server, events,
                new ChangePointerConfig(pageRows, settle, cpEvents));
    }

    /** "ashost" -> "jco.client.ashost"; full property names pass through. */
    static String property(String key) {
        return key.contains(".") ? key : "jco.client." + key;
    }

    /** The destination without credentials, for checks and logs. */
    public String where() {
        String host = destination.getOrDefault("jco.client.ashost", destination.getOrDefault("jco.client.mshost", "?"));
        return host + " client " + destination.getOrDefault("jco.client.client", "?") + " as " + destination.getOrDefault("jco.client.user", "?");
    }

    @Override
    public String toString() {
        return "EccConfig[" + where() + ", salesOrg=" + salesOrg + "]";
    }
}
