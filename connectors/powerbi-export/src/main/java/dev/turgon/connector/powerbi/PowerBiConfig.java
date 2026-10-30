package dev.turgon.connector.powerbi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.EndpointConfig;
import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An endpoint's configuration (the connection's {@code config}):
 *
 * <pre>
 * tenant: 00000000-0000-0000-0000-000000000000    # the Microsoft Entra tenant (directory) ID
 * clientId: 11111111-1111-1111-1111-111111111111  # the service principal's application ID
 * workspace: 22222222-2222-2222-2222-222222222222 # the workspace (group) ID; omitted: My workspace
 * dataset: 33333333-3333-3333-3333-333333333333   # the push dataset's ID, or:
 * datasetName: Turgon Sales                       # its name in the workspace, found by name
 * create: true                                    # create the dataset, and tables it lacks, on the first write
 * retention: none                                 # a created dataset's retention: none or basicFIFO (the newest 200,000 rows)
 * table: SalesOrders                              # the table a payload without "table" goes to
 * tables:
 *   SalesOrders:
 *     columns: { OrderId: String, OrderDate: DateTime, NetValue: Double, Lines: Int64, Reversal: Boolean, TurgonKey: String }
 *     key: TurgonKey                              # filled with the write's idempotency key
 *     reverse: { negate: [NetValue, Lines], set: { Reversal: true } }
 * authority: https://login.microsoftonline.com    # defaults; national clouds and tests change them
 * api: https://api.powerbi.com
 * </pre>
 *
 * The secret is the service principal's client secret. The columns are the
 * table's schema as the push dataset was created with it, or is created
 * with: rows are checked against it before they are sent, which is also
 * the dry run.
 */
public record PowerBiConfig(URI authority, URI api, String tenant, String clientId, String workspace, String dataset,
        String datasetName, boolean create, String retention, String table, Map<String, Table> tables) {

    /** Power BI's push dataset column types. */
    public enum Type {
        INT64("Int64"), DOUBLE("Double"), DECIMAL("Decimal"), BOOLEAN("Boolean"), DATETIME("DateTime"), STRING("String");

        final String wire;

        Type(String wire) {
            this.wire = wire;
        }

        static Type of(String s) {
            for (Type t : values()) {
                if (t.wire.equalsIgnoreCase(s) || (t == DATETIME && s.equalsIgnoreCase("Date"))) {
                    return t;
                }
            }
            return null;
        }
    }

    /**
     * A table of the dataset.
     *
     * @param key the String column that carries each row's idempotency key, or ""
     * @param negate the numeric columns a reversal negates
     * @param set the values a reversal sets, such as a flag column
     */
    public record Table(String name, Map<String, Type> columns, String key, List<String> negate, ObjectNode set) {
        public boolean reversible() {
            return !negate.isEmpty();
        }
    }

    public static PowerBiConfig from(EndpointConfig e) throws ConnectorException.Invalid {
        JsonNode c = e.config();
        String tenant = required(c, "tenant"), clientId = required(c, "clientId");
        String dataset = c.path("dataset").asText("").trim(), datasetName = c.path("datasetName").asText("").trim();
        if (dataset.isEmpty() == datasetName.isEmpty()) {
            throw new ConnectorException.Invalid("set dataset (the push dataset's ID) or datasetName (its name in the workspace), not both");
        }
        boolean create = c.path("create").asBoolean(false);
        String retention = c.path("retention").asText("none");
        if (!retention.equals("none") && !retention.equals("basicFIFO")) {
            throw new ConnectorException.Invalid("retention must be none or basicFIFO");
        }
        Map<String, Table> tables = new LinkedHashMap<>();
        for (var t : c.path("tables").properties()) {
            String name = t.getKey();
            JsonNode tc = t.getValue();
            Map<String, Type> cols = new LinkedHashMap<>();
            for (var col : tc.path("columns").properties()) {
                Type type = Type.of(col.getValue().asText());
                if (type == null) {
                    throw new ConnectorException.Invalid("tables." + name + ".columns." + col.getKey()
                            + ": the type must be Int64, Double, Decimal, Boolean, DateTime or String");
                }
                cols.put(col.getKey(), type);
            }
            if (cols.isEmpty()) {
                throw new ConnectorException.Invalid("tables." + name + ".columns must list the table's columns and their types");
            }
            String key = tc.path("key").asText("");
            if (!key.isEmpty() && cols.get(key) != Type.STRING) {
                throw new ConnectorException.Invalid("tables." + name + ".key: " + key + " must be one of the table's String columns");
            }
            List<String> negate = new ArrayList<>();
            for (JsonNode n : tc.path("reverse").path("negate")) {
                Type type = cols.get(n.asText());
                if (type == null || !(type == Type.INT64 || type == Type.DOUBLE || type == Type.DECIMAL)) {
                    throw new ConnectorException.Invalid("tables." + name + ".reverse.negate: " + n.asText() + " must be one of the table's numeric columns");
                }
                negate.add(n.asText());
            }
            ObjectNode set = JsonNodeFactory.instance.objectNode();
            JsonNode s = tc.path("reverse").path("set");
            for (var f : s.properties()) {
                if (!cols.containsKey(f.getKey())) {
                    throw new ConnectorException.Invalid("tables." + name + ".reverse.set: " + f.getKey() + " is not a column of the table");
                }
                set.set(f.getKey(), f.getValue());
            }
            tables.put(name, new Table(name, cols, key, List.copyOf(negate), set));
        }
        if (tables.isEmpty()) {
            throw new ConnectorException.Invalid("tables must describe at least one table of the push dataset (its columns and their types)");
        }
        String table = c.path("table").asText("");
        if (table.isEmpty() && tables.size() == 1) {
            table = tables.keySet().iterator().next();
        }
        if (!table.isEmpty() && !tables.containsKey(table)) {
            throw new ConnectorException.Invalid("table: " + table + " is not one of tables");
        }
        return new PowerBiConfig(uri(c, "authority", "https://login.microsoftonline.com"), uri(c, "api", "https://api.powerbi.com"),
                tenant, clientId, c.path("workspace").asText(""), dataset, datasetName, create, retention, table, tables);
    }

    private static String required(JsonNode c, String field) throws ConnectorException.Invalid {
        String v = c.path(field).asText("").trim();
        if (v.isEmpty()) {
            throw new ConnectorException.Invalid(field + " is required");
        }
        return v;
    }

    private static URI uri(JsonNode c, String field, String def) throws ConnectorException.Invalid {
        String v = c.path(field).asText(def).replaceAll("/+$", "");
        try {
            URI u = URI.create(v);
            if (u.getScheme() == null || u.getHost() == null) {
                throw new IllegalArgumentException();
            }
            return u;
        } catch (IllegalArgumentException x) {
            throw new ConnectorException.Invalid(field + ": not an absolute URL");
        }
    }

    /** The table a payload names, or the default one. */
    public Table table(String name) throws ConnectorException.Invalid {
        String n = name == null || name.isEmpty() ? table : name;
        if (n.isEmpty()) {
            throw new ConnectorException.Invalid("the payload names no table and the connection has several: set table, or config.table");
        }
        Table t = tables.get(n);
        if (t == null) {
            throw new ConnectorException.Invalid("table " + n + " is not configured (tables: " + String.join(", ", tables.keySet()) + ")");
        }
        return t;
    }

    /** The workspace's datasets' path below the API's root. */
    public String datasetsPath() {
        return "/v1.0/myorg" + (workspace.isEmpty() ? "" : "/groups/" + encode(workspace)) + "/datasets";
    }

    /** A dataset's path below the API's root. */
    public String datasetPath(String id) {
        return datasetsPath() + "/" + encode(id);
    }

    /** The dataset as the configuration names it, for messages. */
    public String datasetLabel() {
        return dataset.isEmpty() ? "named " + datasetName : dataset;
    }

    static String encode(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Checks a row against the table's columns and converts its values to
     * the column types. Messages name the row and column, never the value.
     */
    public static ObjectNode coerce(Table t, JsonNode row, int index) throws ConnectorException.Invalid {
        if (row == null || !row.isObject()) {
            throw new ConnectorException.Invalid("row " + index + " is not an object");
        }
        ObjectNode out = JsonNodeFactory.instance.objectNode();
        for (var f : row.properties()) {
            String col = f.getKey();
            Type type = t.columns().get(col);
            if (type == null) {
                throw new ConnectorException.Invalid("row " + index + ": column " + col + " is not in table " + t.name()
                        + " (columns: " + String.join(", ", t.columns().keySet()) + ")");
            }
            JsonNode v = f.getValue();
            if (v == null || v.isNull()) {
                out.putNull(col);
                continue;
            }
            try {
                switch (type) {
                    case STRING -> {
                        if (!v.isValueNode()) {
                            throw new IllegalArgumentException();
                        }
                        out.put(col, v.asText());
                    }
                    case INT64 -> out.put(col, v.isIntegralNumber() ? v.longValue() : new BigDecimal(v.asText().trim()).longValueExact());
                    case DOUBLE, DECIMAL -> {
                        if (v.isBoolean() || v.isContainerNode()) {
                            throw new IllegalArgumentException();
                        }
                        out.put(col, v.isNumber() ? v.decimalValue() : new BigDecimal(v.asText().trim()));
                    }
                    case BOOLEAN -> {
                        if (v.isBoolean()) {
                            out.put(col, v.booleanValue());
                        } else if (v.asText().equalsIgnoreCase("true") || v.asText().equalsIgnoreCase("false")) {
                            out.put(col, Boolean.parseBoolean(v.asText().toLowerCase(Locale.ROOT)));
                        } else {
                            throw new IllegalArgumentException();
                        }
                    }
                    case DATETIME -> out.put(col, dateTime(v.asText().trim()));
                }
            } catch (IllegalArgumentException | ArithmeticException | DateTimeParseException x) {
                throw new ConnectorException.Invalid("row " + index + ": column " + col + " must be " + type.wire
                        + (type == Type.DATETIME ? " (ISO 8601, such as 2026-10-01 or 2026-10-01T12:00:00Z)" : ""));
            }
        }
        return out;
    }

    /** An ISO 8601 date or date and time, as Power BI takes it. */
    static String dateTime(String s) {
        if (s.length() == 10) {
            return LocalDate.parse(s) + "T00:00:00";
        }
        try {
            return OffsetDateTime.parse(s).toString();
        } catch (DateTimeParseException x) {
            return LocalDateTime.parse(s).toString();
        }
    }

    @Override
    public String toString() {
        return "PowerBiConfig[tenant=" + tenant + ", clientId=" + clientId + ", dataset " + datasetLabel() + "]";
    }
}
