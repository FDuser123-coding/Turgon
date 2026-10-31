package dev.turgon.connector.powerbi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * One push dataset. Operations:
 *
 * <ul>
 * <li>{@code push-rows}: adds rows to a table. The payload is a row, or
 * {@code {"table": ..., "rows": [...]}}. The result carries the rows as
 * sent, which is what its compensation needs.
 * <li>{@code reverse-rows}: push-rows' compensation. Power BI cannot delete
 * single rows, so a reversal adds each row again with its measures negated
 * (and, say, a flag set), the way a ledger reverses a booking: sums over
 * the table come back to what they were.
 * <li>{@code delete-rows}: empties a table (Power BI deletes all rows or none).
 * </ul>
 *
 * Power BI cannot tell a retried push from a new one. The worker never
 * repeats a write that succeeded; one that failed in flight may have added
 * its rows, so a table's key column carries each row's idempotency key for
 * reports to count rows once.
 *
 * <p>The dataset is configured by ID, or found by name. With {@code create},
 * the first write creates a dataset that is not there, with the configured
 * tables, and adds the tables an existing one lacks; a check or a dry run
 * never creates anything.
 */
final class PowerBiEndpoint implements Endpoint {
    /** Power BI's limit on rows per request. */
    static final int BATCH = 10_000;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final PowerBiConfig cfg;
    private final PowerBiClient client;
    private String datasetId; // resolved: configured, found by name, or created
    private boolean tablesReady; // with create: the configured tables are in the dataset

    PowerBiEndpoint(String name, PowerBiConfig cfg, PowerBiClient client) {
        this.name = name;
        this.cfg = cfg;
        this.client = client;
    }

    /** The rows a write sends, checked against the table and keyed. */
    private record Batch(PowerBiConfig.Table table, List<ObjectNode> rows) {
        ObjectNode result() {
            ObjectNode r = JSON.createObjectNode().put("table", table.name()).put("count", rows.size());
            r.putArray("rows").addAll(rows);
            return r;
        }
    }

    private Batch rows(JsonNode payload, String key) throws ConnectorException.Invalid {
        if (payload == null || !payload.isObject()) {
            throw new ConnectorException.Invalid("the payload must be a row or {\"table\", \"rows\"}");
        }
        boolean wrapped = payload.has("rows") && payload.get("rows").isArray();
        PowerBiConfig.Table t = cfg.table(wrapped ? payload.path("table").asText("") : "");
        List<JsonNode> in = new ArrayList<>();
        if (wrapped) {
            payload.get("rows").forEach(in::add);
        } else {
            in.add(payload);
        }
        if (in.isEmpty()) {
            throw new ConnectorException.Invalid("the payload has no rows");
        }
        List<ObjectNode> out = new ArrayList<>();
        for (int i = 0; i < in.size(); i++) {
            ObjectNode row = PowerBiConfig.coerce(t, in.get(i), i + 1);
            if (!t.key().isEmpty() && key != null) {
                row.put(t.key(), in.size() == 1 ? key : key + "/" + (i + 1));
            }
            out.add(row);
        }
        return new Batch(t, out);
    }

    /** A push-rows result's rows, reversed. */
    private Batch reversal(JsonNode pushed) throws ConnectorException.Invalid {
        JsonNode rows = pushed == null ? null : pushed.get("rows");
        if (rows == null || !rows.isArray()) {
            throw new ConnectorException.Invalid("reverse-rows takes push-rows' result: {\"table\", \"rows\"}");
        }
        PowerBiConfig.Table t = cfg.table(pushed.path("table").asText(""));
        if (!t.reversible()) {
            throw new ConnectorException.Invalid("table " + t.name() + " has no reverse.negate: the connector cannot tell which columns to negate");
        }
        List<ObjectNode> out = new ArrayList<>();
        int i = 0;
        for (JsonNode r : rows) {
            ObjectNode row = PowerBiConfig.coerce(t, r, ++i);
            for (String c : t.negate()) {
                JsonNode v = row.get(c);
                if (v != null && v.isNumber()) {
                    row.put(c, v.decimalValue().negate());
                }
            }
            row.setAll(PowerBiConfig.coerce(t, t.set(), i));
            if (!t.key().isEmpty() && row.hasNonNull(t.key())) {
                row.put(t.key(), row.get(t.key()).asText() + "#reversed");
            }
            out.add(row);
        }
        return new Batch(t, out);
    }

    private Batch prepare(String operation, String key, JsonNode payload) throws ConnectorException.Invalid {
        return switch (operation) {
            case "push-rows" -> rows(payload, key);
            case "reverse-rows" -> reversal(payload);
            default -> throw new ConnectorException.Invalid("the connector has no operation " + operation);
        };
    }

    @Override
    public JsonNode simulate(String operation, JsonNode payload) throws Exception {
        if (operation.equals("delete-rows")) {
            PowerBiConfig.Table t = cfg.table(payload == null ? "" : payload.path("table").asText(""));
            return JSON.createObjectNode().put("table", t.name()).put("deletesAllRows", true);
        }
        // The key is the worker's to give: preview the rows without it.
        return prepare(operation, null, payload).result().put("simulated", true);
    }

    @Override
    public JsonNode commit(String operation, String key, JsonNode payload) throws Exception {
        if (operation.equals("delete-rows")) {
            PowerBiConfig.Table t = cfg.table(payload == null ? "" : payload.path("table").asText(""));
            send("DELETE", cfg.datasetPath(dataset(true)) + "/tables/" + PowerBiConfig.encode(t.name()) + "/rows", null);
            return JSON.createObjectNode().put("table", t.name()).put("deleted", true);
        }
        Batch b = prepare(operation, key, payload);
        String path = cfg.datasetPath(dataset(true)) + "/tables/" + PowerBiConfig.encode(b.table().name()) + "/rows";
        for (int from = 0; from < b.rows().size(); from += BATCH) {
            ObjectNode body = JSON.createObjectNode();
            body.putArray("rows").addAll(b.rows().subList(from, Math.min(b.rows().size(), from + BATCH)));
            send("POST", path, body);
        }
        return b.result();
    }

    /**
     * The dataset's ID. A name is looked up in the workspace; when writing
     * with create, a dataset that is not there is created and tables it
     * lacks are added. Null when the named dataset is not there and this
     * is not a write that may create it.
     */
    private synchronized String dataset(boolean write) throws Exception {
        if (datasetId == null && !cfg.dataset().isEmpty()) {
            datasetId = cfg.dataset();
        }
        if (datasetId == null) {
            List<String> ids = named();
            if (ids.size() > 1 && !cfg.create()) {
                throw new ConnectorException.Invalid("the workspace has " + ids.size() + " datasets named " + cfg.datasetName()
                        + ": set dataset to the ID of the one to write to");
            }
            if (!ids.isEmpty()) {
                datasetId = ids.getFirst(); // with create, several writers that each created one agree on the same
            } else if (!write) {
                return null;
            } else if (!cfg.create()) {
                throw new ConnectorException.Invalid("the workspace has no dataset named " + cfg.datasetName()
                        + ": create it, or set create: true to have the connector create it");
            } else {
                ObjectNode body = JSON.createObjectNode().put("name", cfg.datasetName()).put("defaultMode", "Push");
                ArrayNode tables = body.putArray("tables");
                cfg.tables().values().forEach(t -> tables.add(schema(t)));
                String policy = cfg.retention().equals("basicFIFO") ? "basicFIFO" : "None";
                JsonNode created = send("POST", cfg.datasetsPath() + "?defaultRetentionPolicy=" + policy, body);
                // Another writer may have created one at the same time: use the same as it.
                List<String> now = named();
                datasetId = now.isEmpty() ? created.path("id").asText() : now.getFirst();
                tablesReady = datasetId.equals(created.path("id").asText());
            }
        }
        if (write && cfg.create() && !tablesReady) {
            Set<String> present = tables(datasetId);
            for (PowerBiConfig.Table t : cfg.tables().values()) {
                if (!present.contains(t.name())) {
                    send("PUT", cfg.datasetPath(datasetId) + "/tables/" + PowerBiConfig.encode(t.name()), schema(t));
                }
            }
            tablesReady = true;
        }
        return datasetId;
    }

    /** The IDs of the workspace's datasets with the configured name, in order. */
    private List<String> named() throws Exception {
        List<String> ids = new ArrayList<>();
        send("GET", cfg.datasetsPath(), null).path("value").forEach(d -> {
            if (cfg.datasetName().equals(d.path("name").asText())) {
                ids.add(d.path("id").asText());
            }
        });
        ids.sort(null);
        return ids;
    }

    /** A table as a push dataset is created with it. */
    private static ObjectNode schema(PowerBiConfig.Table t) {
        ObjectNode o = JSON.createObjectNode().put("name", t.name());
        ArrayNode cols = o.putArray("columns");
        t.columns().forEach((c, type) -> cols.addObject().put("name", c).put("dataType", type.wire));
        return o;
    }

    private JsonNode send(String method, String path, JsonNode body) throws Exception {
        try {
            return client.call(method, path, body);
        } catch (Exception e) {
            throw PowerBiClient.classify(e);
        }
    }

    @Override
    public List<CheckResult> check() {
        List<CheckResult> out = new ArrayList<>();
        try {
            client.token();
            out.add(CheckResult.pass("sign-in", "application " + cfg.clientId() + " in tenant " + cfg.tenant()));
        } catch (PowerBiClient.SignInError e) {
            out.add(CheckResult.fail("sign-in", e.getMessage(), signInFix(e)));
            return out;
        } catch (Exception e) {
            out.add(CheckResult.fail("sign-in", e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "Microsoft Entra did not answer at " + cfg.authority() + ". Allow outbound HTTPS to it from this node."));
            return out;
        }
        Set<String> present;
        String id;
        try {
            id = dataset(false);
        } catch (Exception e) {
            out.add(CheckResult.fail("dataset", e.getMessage(), apiFix(e)));
            return out;
        }
        if (id == null) {
            if (cfg.create()) {
                out.add(CheckResult.pass("dataset", "no dataset named " + cfg.datasetName() + " yet: the first write creates it with tables "
                        + String.join(", ", cfg.tables().keySet())));
            } else {
                out.add(CheckResult.fail("dataset", "the workspace has no dataset named " + cfg.datasetName(),
                        "Create the push dataset, or set create: true to have the connector create it on the first write."));
            }
            return out;
        }
        try {
            JsonNode d = client.call("GET", cfg.datasetPath(id), null);
            if (!d.path("addRowsAPIEnabled").asBoolean(false)) {
                out.add(CheckResult.fail("dataset", d.path("name").asText(id) + " is not a push dataset",
                        "Rows can only be added to push datasets (or streaming datasets with historic data analysis on). "
                                + "Create one through the REST API (Push Datasets - PostDatasetInGroup) with the tables in the connection's config,"
                                + " or name a new one with datasetName and create: true."));
                return out;
            }
            out.add(CheckResult.pass("dataset", d.path("name").asText(id) + " (push dataset " + id + ")"));
            present = tables(id);
        } catch (Exception e) {
            out.add(CheckResult.fail("dataset", e.getMessage(), apiFix(e)));
            return out;
        }
        for (PowerBiConfig.Table t : cfg.tables().values()) {
            if (!present.contains(t.name()) && cfg.create()) {
                out.add(CheckResult.pass("table " + t.name(), "not in the dataset yet: the first write adds it"));
            } else if (!present.contains(t.name())) {
                out.add(CheckResult.fail("table " + t.name(), "not in the dataset (it has: " + String.join(", ", present) + ")",
                        "Add the table to the push dataset (Push Datasets - PutTableInGroup), correct its name in the connection's config,"
                                + " or set create: true to have the connector add it."));
                continue;
            }
            out.add(CheckResult.pass("table " + t.name(), t.columns().size() + " columns" + (t.key().isEmpty() ? "" : ", key " + t.key())));
            if (t.reversible()) {
                out.add(CheckResult.pass("reverse " + t.name(), "negates " + String.join(", ", t.negate())));
            } else {
                out.add(CheckResult.fail("reverse " + t.name(), "no columns to negate: a failed run cannot undo the rows it pushed",
                        "Set tables." + t.name() + ".reverse.negate to the table's measures (amounts, quantities)."));
            }
        }
        return out;
    }

    private Set<String> tables(String id) throws Exception {
        Set<String> present = new LinkedHashSet<>();
        client.call("GET", cfg.datasetPath(id) + "/tables", null).path("value").forEach(v -> present.add(v.path("name").asText()));
        return present;
    }

    private static String signInFix(PowerBiClient.SignInError e) {
        return switch (e.aadsts) {
            case "7000215", "7000216", "7000222" -> "The client secret is wrong or expired. Create a new secret for the app registration and store its value (not its ID) in the secret.";
            case "700016" -> "No app registration with this client ID in the tenant. Check config.clientId and config.tenant.";
            case "90002", "900023" -> "The tenant does not exist. Check config.tenant (the directory ID).";
            default -> "Check the app registration's client ID and secret in Microsoft Entra.";
        };
    }

    private static String apiFix(Exception e) {
        if (e instanceof PowerBiClient.ApiError a) {
            return switch (a.status) {
                case 401, 403 -> "Allow service principals to use Power BI APIs (tenant settings, Developer settings), and add the app to the workspace as Member or Contributor.";
                case 404 -> "No such dataset in the workspace, or the service principal cannot see it. Check config.workspace and config.dataset.";
                default -> "";
            };
        }
        return "Power BI did not answer. Allow outbound HTTPS to api.powerbi.com from this node.";
    }

    /**
     * The configured tables, with the columns their config declares (Power
     * BI does not list a push dataset's columns). A configured table the
     * dataset lacks is left out, so drift shows what uses it.
     */
    @Override
    public JsonNode discover(List<String> objects) throws Exception {
        String id = dataset(false);
        Set<String> present = id == null ? Set.of() : tables(id);
        ObjectNode cat = JSON.createObjectNode();
        ArrayNode objs = cat.putArray("objects"), uses = cat.putArray("uses");
        for (PowerBiConfig.Table t : cfg.tables().values()) {
            if (!present.contains(t.name())) {
                continue;
            }
            ObjectNode o = objs.addObject().put("name", t.name()).put("kind", "table");
            ArrayNode fields = o.putArray("fields");
            t.columns().forEach((c, type) -> {
                ObjectNode f = fields.addObject().put("name", c).put("type", type.wire);
                if (c.equals(t.key())) {
                    f.put("key", true);
                }
            });
            for (String c : t.columns().keySet()) {
                uses.addObject().put("object", t.name()).put("field", c).put("by", "operation push-rows").put("creates", true);
            }
        }
        for (String o : objects) {
            if (!cfg.tables().containsKey(o) && present.contains(o)) {
                objs.addObject().put("name", o).put("kind", "table").putArray("fields");
            }
        }
        return cat;
    }

    @Override
    public String toString() {
        return name + " " + cfg;
    }
}
