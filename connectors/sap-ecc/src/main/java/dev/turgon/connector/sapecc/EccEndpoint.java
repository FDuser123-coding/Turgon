package dev.turgon.connector.sapecc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.turgon.connector.sdk.CamelEndpoint;
import dev.turgon.connector.sdk.CheckResult;
import dev.turgon.connector.sdk.ConnectorException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

/**
 * One ECC system. Each operation is a Camel route that calls BAPIs:
 *
 * <ul>
 *   <li>create-sales-order: BAPI_SALESORDER_CREATEFROMDAT2, with TESTRUN to
 *       simulate. The idempotency key is the purchase order number
 *       (PURCH_NO_C); a retried create first looks it up with
 *       BAPI_SALESORDER_GETLIST and returns the order it finds. The result is
 *       {@code {"salesOrder": number, "id": number}}. The create
 *       and BAPI_TRANSACTION_COMMIT run in one session. Confirmed with
 *       BAPI_SALESORDER_GETSTATUS.
 *   <li>cancel-sales-order: deletes the order (BAPI_SALESORDER_CHANGE,
 *       UPDATEFLAG D); SAP refuses once it has follow-on documents.
 *   <li>get-customer: BAPI_CUSTOMER_GETDETAIL2.
 * </ul>
 */
final class EccEndpoint extends CamelEndpoint {
    static final List<String> FUNCTIONS = List.of("BAPI_SALESORDER_CREATEFROMDAT2", "BAPI_SALESORDER_GETLIST",
            "BAPI_SALESORDER_GETSTATUS", "BAPI_SALESORDER_CHANGE", "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK",
            "BAPI_CUSTOMER_GETDETAIL2");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final EccConfig cfg;
    private final Rfc rfc;
    private final Idocs idocs;
    private final IdocServerStarter starter;
    private AutoCloseable server; // the registered server program, while a stream is open

    /** Registers the server program at the gateway (RfcProvider.serve). */
    @FunctionalInterface
    interface IdocServerStarter {
        AutoCloseable start(IdocReceiver receiver) throws Exception;
    }

    EccEndpoint(String name, EccConfig cfg, Rfc rfc, IdocServerStarter starter) throws Exception {
        super(name, routes(cfg, rfc));
        this.cfg = cfg;
        this.rfc = rfc;
        this.starter = starter;
        this.idocs = new Idocs(rfc, cfg.idocEvents());
    }

    /** The structure fields each operation fills or reads, for the metadata graph. */
    static final Map<String, Map<String, List<String>>> USED = Map.of(
            "BAPISDHD1", Map.of("operation create-sales-order", List.of("DOC_TYPE", "SALES_ORG", "DISTR_CHAN", "DIVISION",
                    "PURCH_NO_C", "REQ_DATE_H", "CURRENCY")),
            "BAPISDITM", Map.of("operation create-sales-order", List.of("ITM_NUMBER", "MATERIAL", "TARGET_QTY")),
            "BAPIPARNR", Map.of("operation create-sales-order", List.of("PARTN_ROLE", "PARTN_NUMB")),
            "BAPISCHDL", Map.of("operation create-sales-order", List.of("ITM_NUMBER", "REQ_QTY")),
            "BAPISDH1X", Map.of("operation cancel-sales-order", List.of("UPDATEFLAG")),
            "BAPICUSTOMER_04", Map.of("operation get-customer", List.of("NAME", "STREET", "POSTL_COD1", "CITY", "COUNTRY")));

    /**
     * What the system holds: the interfaces of the BAPIs the connector
     * calls (RFC_GET_FUNCTION_INTERFACE), the fields of the structures it
     * fills (DDIF_FIELDINFO_GET), and the segments of the configured IDoc
     * types (IDOCTYPE_READ_COMPLETE). More objects are named
     * FUNCTION:name, STRUCTURE:name or IDOC:type.
     */
    @Override
    public JsonNode discover(List<String> objects) throws Exception {
        ObjectNode cat = JSON.createObjectNode();
        ArrayNode objs = cat.putArray("objects"), uses = cat.putArray("uses");
        java.util.Set<String> functions = new java.util.TreeSet<>(FUNCTIONS), structures = new java.util.TreeSet<>(USED.keySet()),
                idocTypes = new java.util.TreeSet<>();
        cfg.idocEvents().values().forEach(e -> idocTypes.addAll(e.idocTypes()));
        for (String o : objects) {
            if (o.startsWith("FUNCTION:")) {
                functions.add(o.substring(9));
            } else if (o.startsWith("IDOC:")) {
                idocTypes.add(o.substring(5));
            } else {
                structures.add(o.startsWith("STRUCTURE:") ? o.substring(10) : o);
            }
        }
        for (String f : functions) {
            Map<String, Object> r = rfc.call("RFC_GET_FUNCTION_INTERFACE", Map.of("FUNCNAME", f));
            ObjectNode o = objs.addObject().put("name", f).put("kind", "function");
            ArrayNode fields = o.putArray("fields");
            for (Map<String, String> p : Rfc.table(r, "PARAMS")) {
                if (p.getOrDefault("FIELDNAME", "").isBlank()) { // a parameter, not one of its fields
                    String cls = p.getOrDefault("PARAMCLASS", "");
                    String tab = p.getOrDefault("TABNAME", "").trim();
                    fields.addObject().put("name", p.get("PARAMETER").trim())
                            .put("type", tab.isEmpty() ? p.getOrDefault("EXID", "").trim() : tab)
                            .put("label", p.getOrDefault("PARAMTEXT", "").trim())
                            .put("required", cls.equals("I") && !"X".equals(p.getOrDefault("OPTIONAL", "").trim()))
                            .put("readOnly", cls.equals("E"));
                }
            }
        }
        for (String st : structures) {
            Map<String, Object> r = rfc.call("DDIF_FIELDINFO_GET", Map.of("TABNAME", st));
            var rows = Rfc.table(r, "DFIES_TAB");
            if (rows.isEmpty()) {
                continue; // not in this system: drift shows what uses it
            }
            ObjectNode o = objs.addObject().put("name", st).put("kind", "structure");
            ArrayNode fields = o.putArray("fields");
            for (Map<String, String> f : rows) {
                fields.addObject().put("name", f.get("FIELDNAME").trim()).put("type", f.getOrDefault("DATATYPE", "").trim())
                        .put("length", parseInt(f.getOrDefault("LENG", "0")))
                        .put("label", f.getOrDefault("FIELDTEXT", "").trim())
                        .put("key", "X".equals(f.getOrDefault("KEYFLAG", "").trim()));
            }
        }
        for (var u : USED.entrySet()) {
            for (var by : u.getValue().entrySet()) {
                for (String field : by.getValue()) {
                    // The create fills these structures: a field that becomes required breaks it.
                    uses.addObject().put("object", u.getKey()).put("field", field).put("by", by.getKey())
                            .put("creates", by.getKey().equals("operation create-sales-order"));
                }
            }
        }
        for (String type : idocTypes) {
            Map<String, Object> r = rfc.call("IDOCTYPE_READ_COMPLETE", Map.of("PI_IDOCTYP", type, "PI_CIMTYP", ""));
            Map<String, ArrayNode> segs = new java.util.TreeMap<>();
            for (Map<String, String> f : Rfc.table(r, "PT_FIELDS")) {
                String seg = f.get("SEGMENTTYP").trim();
                ArrayNode fields = segs.computeIfAbsent(seg, k -> {
                    ObjectNode o = objs.addObject().put("name", type + "/" + k).put("kind", "idoc-segment");
                    return o.putArray("fields");
                });
                fields.addObject().put("name", f.get("FIELDNAME").trim()).put("type", f.getOrDefault("DATATYPE", "CHAR").trim())
                        .put("length", parseInt(f.getOrDefault("EXTLEN", "0")))
                        .put("label", f.getOrDefault("DESCRP", "").trim());
            }
            for (var e : cfg.idocEvents().entrySet()) {
                if (e.getValue().idocTypes().contains(type)) {
                    for (String seg : segs.keySet()) {
                        uses.addObject().put("object", type + "/" + seg).put("field", "").put("by", "event " + e.getKey());
                    }
                }
            }
        }
        return cat;
    }

    private static int parseInt(String s) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public java.util.Set<String> streams() {
        return cfg.idocEvents().keySet();
    }

    /**
     * The server program is registered while a worker listens, so SAP sends
     * IDocs only to a sidecar whose worker holds the subscription.
     */
    @Override
    public synchronized AutoCloseable stream(String event, byte[] resume, dev.turgon.connector.sdk.StreamSink sink) throws Exception {
        idocs.open(event, sink);
        if (server == null) {
            try {
                server = starter.start(idocs);
            } catch (Exception e) {
                idocs.close(event, sink);
                throw e;
            }
        }
        return () -> {
            synchronized (this) {
                idocs.close(event, sink);
                if (idocs.idle() && server != null) {
                    server.close();
                    server = null;
                }
            }
        };
    }

    private static RouteBuilder routes(EccConfig cfg, Rfc rfc) {
        Orders orders = new Orders(cfg, rfc);
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:create-sales-order").routeId("create-sales-order")
                        .choice()
                            .when(header(MODE).isEqualTo("simulate")).process(orders::simulate)
                            .when(header(MODE).isEqualTo("confirm")).process(orders::confirm)
                            .when(header(MODE).isEqualTo("commit")).process(orders::create)
                            .otherwise().process(Orders::unsupported);
                from("direct:cancel-sales-order").routeId("cancel-sales-order")
                        .choice()
                            .when(header(MODE).isEqualTo("commit")).process(orders::cancel)
                            .otherwise().process(Orders::unsupported);
                from("direct:get-customer").routeId("get-customer")
                        .choice()
                            .when(header(MODE).isEqualTo("read")).process(orders::customer)
                            .otherwise().process(Orders::unsupported);
            }
        };
    }

    @Override
    public void confirm(String operation, JsonNode result) throws Exception {
        if (operation.equals("create-sales-order")) {
            super.confirm(operation, result);
        }
    }

    @Override
    public List<CheckResult> check() {
        List<CheckResult> out = new ArrayList<>();
        try {
            rfc.ping();
            out.add(CheckResult.pass("rfc", rfc.describe() + ": RFC_PING answered"));
        } catch (Exception e) {
            out.add(CheckResult.fail("rfc", rfc.describe() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage(),
                    "Check the destination (ashost, sysnr, client), the RFC user and its password in the secret, "
                            + "and that the sidecar reaches the gateway port (33<sysnr>)."));
            return out;
        }
        for (String f : FUNCTIONS) {
            try {
                if (rfc.exists(f)) {
                    out.add(CheckResult.pass(f, "available"));
                } else {
                    out.add(CheckResult.fail(f, "not found", "This release lacks " + f + "; the connector needs ECC 6.0."));
                }
            } catch (Exception e) {
                out.add(CheckResult.fail(f, e.getClass().getSimpleName() + ": " + e.getMessage(),
                        "Grant the RFC user S_RFC for " + f + "'s function group, and the sales authorizations (V_VBAK_AAT, V_VBAK_VKO)."));
            }
        }
        out.add(CheckResult.pass("sales area", cfg.salesOrg() + "/" + cfg.distributionChannel() + "/" + cfg.division()
                + ", order type " + cfg.orderType()));
        if (!cfg.idocEvents().isEmpty()) {
            String program = cfg.server().get("jco.server.progid") + " at " + cfg.server().get("jco.server.gwhost") + " "
                    + cfg.server().get("jco.server.gwserv");
            try {
                if (!rfc.exists("IDOCTYPE_READ_COMPLETE")) {
                    out.add(CheckResult.fail("IDOCTYPE_READ_COMPLETE", "not found", "The connector reads IDoc segment definitions with it."));
                } else {
                    out.add(CheckResult.pass("idoc", "events " + String.join(", ", cfg.idocEvents().keySet()) + " from server program " + program
                            + (server != null ? ", registered" : ", registered while a worker listens")));
                }
            } catch (Exception e) {
                out.add(CheckResult.fail("IDOCTYPE_READ_COMPLETE", e.getClass().getSimpleName() + ": " + e.getMessage(),
                        "Grant the RFC user S_RFC for function group EDIMEXT (IDoc type metadata)."));
            }
        }
        return out;
    }

    @Override
    public void close() throws Exception {
        try {
            synchronized (this) {
                if (server != null) {
                    server.close();
                    server = null;
                }
            }
            super.close();
        } finally {
            rfc.close();
        }
    }

    /** The BAPI calls, as Camel processors. */
    static final class Orders {
        private final EccConfig cfg;
        private final Rfc rfc;

        Orders(EccConfig cfg, Rfc rfc) {
            this.cfg = cfg;
            this.rfc = rfc;
        }

        static void unsupported(Exchange ex) throws Exception {
            throw new ConnectorException.Unsupported(ex.getIn().getHeader(MODE, String.class) + " of "
                    + ex.getIn().getHeader(OPERATION, String.class));
        }

        /** The order's BAPI parameters from Turgon's SalesOrder. */
        Map<String, Object> params(JsonNode o, String purchaseOrder) throws ConnectorException.Invalid {
            if (o == null || !o.isObject()) {
                throw new ConnectorException.Invalid("the payload must be a JSON object");
            }
            String customer = customer(o.path("customerId").asText(""));
            Map<String, String> header = new LinkedHashMap<>();
            header.put("DOC_TYPE", cfg.orderType());
            header.put("SALES_ORG", cfg.salesOrg());
            header.put("DISTR_CHAN", cfg.distributionChannel());
            header.put("DIVISION", cfg.division());
            if (purchaseOrder != null) {
                header.put("PURCH_NO_C", purchaseOrder);
            }
            String date = o.path("orderDate").asText("");
            if (!date.isEmpty()) {
                try {
                    header.put("REQ_DATE_H", LocalDate.parse(date.length() > 10 ? date.substring(0, 10) : date).toString().replace("-", ""));
                } catch (DateTimeParseException e) {
                    throw new ConnectorException.Invalid("orderDate must be a date (YYYY-MM-DD)");
                }
            }
            String currency = o.path("currency").asText("");
            if (!currency.isEmpty()) {
                header.put("CURRENCY", currency.toUpperCase());
            }
            JsonNode lines = o.path("lines");
            if (!lines.isArray() || lines.isEmpty()) {
                throw new ConnectorException.Invalid("lines must list at least one item");
            }
            List<Map<String, String>> items = new ArrayList<>();
            List<Map<String, String>> schedules = new ArrayList<>();
            int n = 0;
            for (JsonNode line : lines) {
                n++;
                String item = String.format("%06d", n * 10);
                String material = line.path("material").asText("");
                if (material.isEmpty()) {
                    throw new ConnectorException.Invalid("lines[" + (n - 1) + "].material is required");
                }
                BigDecimal qty;
                try {
                    qty = new BigDecimal(line.path("quantity").asText(""));
                } catch (NumberFormatException e) {
                    throw new ConnectorException.Invalid("lines[" + (n - 1) + "].quantity must be a number");
                }
                if (qty.signum() <= 0) {
                    throw new ConnectorException.Invalid("lines[" + (n - 1) + "].quantity must be positive");
                }
                items.add(Map.of("ITM_NUMBER", item, "MATERIAL", material.toUpperCase(), "TARGET_QTY", qty.toPlainString()));
                schedules.add(Map.of("ITM_NUMBER", item, "REQ_QTY", qty.toPlainString()));
            }
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("ORDER_HEADER_IN", header);
            p.put("ORDER_PARTNERS", List.of(Map.of("PARTN_ROLE", cfg.partnerRole(), "PARTN_NUMB", customer)));
            p.put("ORDER_ITEMS_IN", items);
            p.put("ORDER_SCHEDULES_IN", schedules);
            return p;
        }

        /** A customer number with ALPHA conversion: numeric ones are zero-padded to 10. */
        static String customer(String id) throws ConnectorException.Invalid {
            if (id.isEmpty()) {
                throw new ConnectorException.Invalid("customerId is required");
            }
            if (id.length() > 10) {
                throw new ConnectorException.Invalid("customerId must be an SAP customer number (at most 10 characters)");
            }
            return id.chars().allMatch(Character::isDigit) ? String.format("%10s", id).replace(' ', '0') : id.toUpperCase();
        }

        static String purchaseOrder(String key) throws ConnectorException.Invalid {
            if (key == null || key.isEmpty() || key.length() > 35) {
                throw new ConnectorException.Invalid("the idempotency key must be 1 to 35 characters to fit the purchase order number");
            }
            return key;
        }

        void simulate(Exchange ex) throws Exception {
            JsonNode o = ex.getIn().getBody(JsonNode.class);
            Map<String, Object> p = params(o, null);
            p.put("TESTRUN", "X");
            Map<String, Object> r = rfc.call("BAPI_SALESORDER_CREATEFROMDAT2", p);
            Bapi.failOnError(r, "the test run");
            ObjectNode out = JSON.createObjectNode();
            out.put("testRun", true);
            out.put("customer", customer(o.path("customerId").asText()));
            out.put("salesArea", cfg.salesOrg() + "/" + cfg.distributionChannel() + "/" + cfg.division());
            out.put("items", ((List<?>) p.get("ORDER_ITEMS_IN")).size());
            out.set("messages", Bapi.messages(r));
            ex.getMessage().setBody(out);
        }

        void create(Exchange ex) throws Exception {
            JsonNode o = ex.getIn().getBody(JsonNode.class);
            String po = purchaseOrder(ex.getIn().getHeader(IDEMPOTENCY_KEY, String.class));
            Map<String, Object> p = params(o, po);
            String customer = customer(o.path("customerId").asText());
            if (cfg.lookupByPurchaseOrder()) {
                String existing = find(customer, po);
                if (existing != null) {
                    ex.getMessage().setBody(JSON.createObjectNode().put("salesOrder", existing).put("id", existing).put("existing", true));
                    return;
                }
            }
            String doc = rfc.session(s -> {
                Map<String, Object> r = s.call("BAPI_SALESORDER_CREATEFROMDAT2", p);
                String number = String.valueOf(r.getOrDefault("SALESDOCUMENT", "")).trim();
                if (Bapi.hasError(r) || number.isEmpty()) {
                    s.call("BAPI_TRANSACTION_ROLLBACK", Map.of());
                    Bapi.failOnError(r, "the create");
                    throw new ConnectorException.Rejected("SAP created no sales order and gave no reason");
                }
                Bapi.failOnError(s.call("BAPI_TRANSACTION_COMMIT", Map.of("WAIT", "X")), "the commit");
                return number;
            });
            ex.getMessage().setBody(JSON.createObjectNode().put("salesOrder", doc).put("id", doc));
        }

        /** The order created for this purchase order number, if any. */
        String find(String customer, String po) throws Exception {
            Map<String, Object> r = rfc.call("BAPI_SALESORDER_GETLIST", Map.of(
                    "CUSTOMER_NUMBER", customer, "SALES_ORGANIZATION", cfg.salesOrg(), "PURCHASE_ORDER_NUMBER", po));
            for (Map<String, String> row : Rfc.table(r, "SALES_ORDERS")) {
                if (po.equals(row.getOrDefault("PURCH_NO_C", po).trim())) {
                    return row.get("SD_DOC");
                }
            }
            return null;
        }

        void confirm(Exchange ex) throws Exception {
            String doc = ex.getIn().getBody(JsonNode.class).path("salesOrder").asText("");
            if (doc.isEmpty()) {
                throw new ConnectorException.Invalid("the result has no salesOrder");
            }
            Map<String, Object> r = rfc.call("BAPI_SALESORDER_GETSTATUS", Map.of("SALESDOCUMENT", doc));
            if (Bapi.hasError(r) || Rfc.table(r, "STATUSINFO").isEmpty()) {
                throw new ConnectorException.NotFound("sales order " + doc + " is not in SAP after the commit");
            }
            ex.getMessage().setBody(JSON.createObjectNode().put("salesOrder", doc));
        }

        void cancel(Exchange ex) throws Exception {
            String doc = ex.getIn().getBody(JsonNode.class).path("salesOrder").asText("");
            if (doc.isEmpty()) {
                throw new ConnectorException.Invalid("cancel-sales-order needs the result of create-sales-order");
            }
            Map<String, Object> status = rfc.call("BAPI_SALESORDER_GETSTATUS", Map.of("SALESDOCUMENT", doc));
            if (Bapi.hasError(status) || Rfc.table(status, "STATUSINFO").isEmpty()) {
                ex.getMessage().setBody(JSON.createObjectNode().put("salesOrder", doc).put("deleted", true).put("alreadyGone", true));
                return;
            }
            rfc.session(s -> {
                Map<String, Object> r = s.call("BAPI_SALESORDER_CHANGE", Map.of(
                        "SALESDOCUMENT", doc, "ORDER_HEADER_INX", Map.of("UPDATEFLAG", "D")));
                if (Bapi.hasError(r)) {
                    s.call("BAPI_TRANSACTION_ROLLBACK", Map.of());
                    Bapi.failOnError(r, "deleting the order");
                }
                Bapi.failOnError(s.call("BAPI_TRANSACTION_COMMIT", Map.of("WAIT", "X")), "the commit");
                return null;
            });
            ex.getMessage().setBody(JSON.createObjectNode().put("salesOrder", doc).put("deleted", true));
        }

        void customer(Exchange ex) throws Exception {
            String id = customer(ex.getIn().getHeader(ID, String.class));
            Map<String, Object> r = rfc.call("BAPI_CUSTOMER_GETDETAIL2", Map.of("CUSTOMERNO", id));
            Map<String, String> addr = Rfc.structure(r, "CUSTOMERADDRESS");
            if (Bapi.hasError(r) || addr.getOrDefault("NAME", "").isBlank()) {
                throw new ConnectorException.NotFound("no customer " + id);
            }
            ObjectNode out = JSON.createObjectNode();
            out.put("id", id);
            out.put("name", addr.getOrDefault("NAME", "").trim());
            out.put("street", addr.getOrDefault("STREET", "").trim());
            out.put("postalCode", addr.getOrDefault("POSTL_COD1", "").trim());
            out.put("city", addr.getOrDefault("CITY", "").trim());
            out.put("country", addr.getOrDefault("COUNTRY", "").trim());
            ex.getMessage().setBody(out);
        }
    }

    /** BAPI return messages (BAPIRET2 and older BAPIRETURN shapes). */
    static final class Bapi {
        private Bapi() {}

        static List<Map<String, String>> returns(Map<String, Object> r) {
            return Rfc.table(r, "RETURN");
        }

        static boolean hasError(Map<String, Object> r) {
            return returns(r).stream().anyMatch(m -> {
                String t = m.getOrDefault("TYPE", "");
                return t.equals("E") || t.equals("A") || t.equals("X");
            });
        }

        static String text(Map<String, String> m) {
            String id = m.getOrDefault("ID", "").trim();
            String num = m.getOrDefault("NUMBER", "").trim();
            String code = m.getOrDefault("CODE", "").trim();
            String ref = !id.isEmpty() ? id + " " + num : code;
            return (ref.isBlank() ? "" : ref + ": ") + m.getOrDefault("MESSAGE", "").trim();
        }

        static void failOnError(Map<String, Object> r, String what) throws ConnectorException.Rejected {
            List<String> errors = returns(r).stream().filter(m -> List.of("E", "A", "X").contains(m.getOrDefault("TYPE", "")))
                    .map(Bapi::text).toList();
            if (!errors.isEmpty()) {
                throw new ConnectorException.Rejected("SAP refused " + what + ": " + String.join("; ", errors));
            }
        }

        static ArrayNode messages(Map<String, Object> r) {
            ArrayNode out = JSON.createArrayNode();
            for (Map<String, String> m : returns(r)) {
                if (!m.getOrDefault("MESSAGE", "").isBlank()) {
                    out.addObject().put("type", m.getOrDefault("TYPE", "")).put("message", text(m));
                }
            }
            return out;
        }
    }
}
