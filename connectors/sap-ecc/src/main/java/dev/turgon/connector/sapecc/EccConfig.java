package dev.turgon.connector.sapecc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.EndpointConfig;
import java.util.LinkedHashMap;
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
 * </pre>
 *
 * The secret is the RFC user: {@code user:password}, or a JSON object of
 * destination properties ({@code {"user": ..., "passwd": ..., "ashost": ...}}).
 * Passwords are refused in the configuration, which is not secret.
 */
public record EccConfig(String provider, Map<String, String> destination, String salesOrg, String distributionChannel,
        String division, String orderType, String partnerRole, boolean lookupByPurchaseOrder, int peakLimit) {

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
        return new EccConfig(c.path("rfc").path("provider").asText(""), new LinkedHashMap<>(dest),
                area.path("salesOrg").asText("1000"), area.path("distributionChannel").asText("10"),
                area.path("division").asText("00"), c.path("orderType").asText("TA"), c.path("partnerRole").asText("AG"),
                c.path("lookupByPurchaseOrder").asBoolean(true), peak);
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
