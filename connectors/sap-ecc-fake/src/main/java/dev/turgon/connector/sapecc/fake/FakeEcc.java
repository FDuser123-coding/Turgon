package dev.turgon.connector.sapecc.fake;

import dev.turgon.connector.sapecc.Rfc;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-memory SAP ECC answering the BAPIs the sap-ecc connector calls,
 * with SAP's rules where they matter: an order is kept only if
 * BAPI_TRANSACTION_COMMIT runs in the same session, a test run saves
 * nothing, customers can be blocked for sales, orders with a delivery
 * cannot be deleted, and unknown parameters fail as in JCo.
 *
 * <p>Customers 1000 (Ada Lovelace GmbH), 1001 (Hopper Labs) and 1002
 * (blocked); materials M-1, M-7, M-8 and M-9; sales area 1000/10/00.
 */
public final class FakeEcc {
    private static final Map<String, FakeEcc> SYSTEMS = new ConcurrentHashMap<>();
    static final Set<String> FUNCTIONS = Set.of("RFC_PING", "BAPI_SALESORDER_CREATEFROMDAT2", "BAPI_SALESORDER_GETLIST",
            "BAPI_SALESORDER_GETSTATUS", "BAPI_SALESORDER_CHANGE", "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK",
            "BAPI_CUSTOMER_GETDETAIL2");

    public record Customer(String name, String street, String postalCode, String city, String country, boolean blocked) {}

    public record Order(String number, String customer, String salesOrg, String purchaseOrder, String date,
            List<Map<String, String>> items, boolean delivered) {}

    private final Map<String, Customer> customers = new ConcurrentHashMap<>();
    private final Set<String> materials = ConcurrentHashMap.newKeySet();
    private final Map<String, Order> orders = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger(12000);
    private final Map<String, Integer> failures = new ConcurrentHashMap<>();
    /** Calls made, by function, for tests. */
    public final List<String> calls = java.util.Collections.synchronizedList(new ArrayList<>());

    public FakeEcc() {
        customers.put("0000001000", new Customer("Ada Lovelace GmbH", "Unter den Linden 1", "10117", "Berlin", "DE", false));
        customers.put("0000001001", new Customer("Hopper Labs", "1 Navy Way", "20301", "Arlington", "US", false));
        customers.put("0000001002", new Customer("Blocked Trading Ltd", "1 Main St", "EC1A", "London", "GB", true));
        materials.addAll(List.of("M-1", "M-7", "M-8", "M-9"));
    }

    /** The system behind a destination host, shared by every connection to it. */
    public static FakeEcc system(String host) {
        return SYSTEMS.computeIfAbsent(host, h -> new FakeEcc());
    }

    public static void reset() {
        SYSTEMS.clear();
    }

    public Map<String, Order> orders() {
        return orders;
    }

    /** Makes the next {@code times} calls of function fail as a broken connection would. */
    public void fail(String function, int times) {
        failures.put(function, times);
    }

    /** Gives the order a delivery, after which SAP refuses to delete it. */
    public void deliver(String number) {
        orders.computeIfPresent(number, (k, o) -> new Order(o.number(), o.customer(), o.salesOrg(), o.purchaseOrder(),
                o.date(), o.items(), true));
    }

    static Map<String, String> msg(String type, String id, String number, String text) {
        return Map.of("TYPE", type, "ID", id, "NUMBER", number, "MESSAGE", text);
    }

    /** A connection as the given user ("locked" fails to log on). */
    public Connection connect(String user) {
        return new Connection(this, user);
    }

    /** A connection: changes wait in the session's logical unit of work until committed. */
    public static final class Connection implements Rfc {
        private final FakeEcc ecc;
        private final String user;
        // Per thread, as JCoContext: set inside a session.
        private final ThreadLocal<List<Runnable>> luw = new ThreadLocal<>();

        Connection(FakeEcc ecc, String user) {
            this.ecc = ecc;
            this.user = user;
        }

        @Override
        public Map<String, Object> call(String function, Map<String, Object> params) throws Exception {
            ecc.calls.add(function);
            Integer left = ecc.failures.get(function);
            if (left != null && left > 0) {
                ecc.failures.put(function, left - 1);
                throw new java.io.IOException("connection reset by the SAP gateway");
            }
            if (user.equals("locked")) {
                throw new IllegalStateException("Name or password is incorrect (repeat logon)");
            }
            List<Runnable> work = luw.get();
            // Outside a session the context ends with the call: uncommitted work is lost, as in SAP.
            return ecc.handle(function, params, work == null ? new ArrayList<>() : work);
        }

        @Override
        public <T> T session(Work<T> work) throws Exception {
            begin();
            try {
                return work.run(this);
            } finally {
                end();
            }
        }

        /** Starts a session (JCoContext.begin). */
        public void begin() {
            luw.set(new ArrayList<>());
        }

        /** Ends it; uncommitted work is lost (JCoContext.end). */
        public void end() {
            luw.remove();
        }

        @Override
        public void ping() throws Exception {
            call("RFC_PING", Map.of());
        }

        @Override
        public boolean exists(String function) {
            return FUNCTIONS.contains(function);
        }

        @Override
        public String describe() {
            return "fake ECC (not a real SAP system)";
        }
    }

    private static void allow(String function, Map<String, Object> params, String... names) {
        Set<String> ok = Set.of(names);
        for (String p : params.keySet()) {
            if (!ok.contains(p)) {
                throw new IllegalArgumentException(function + " has no parameter " + p);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T get(Map<String, Object> params, String name, T empty) {
        Object v = params.get(name);
        return v == null ? empty : (T) v;
    }

    Map<String, Object> handle(String function, Map<String, Object> p, List<Runnable> luw) {
        Map<String, Object> out = new LinkedHashMap<>();
        switch (function) {
            case "RFC_PING" -> allow(function, p);
            case "BAPI_TRANSACTION_COMMIT" -> {
                allow(function, p, "WAIT");
                luw.forEach(Runnable::run);
                luw.clear();
                out.put("RETURN", Map.of());
            }
            case "BAPI_TRANSACTION_ROLLBACK" -> {
                allow(function, p);
                luw.clear();
                out.put("RETURN", Map.of());
            }
            case "BAPI_SALESORDER_CREATEFROMDAT2" -> create(p, luw, out);
            case "BAPI_SALESORDER_GETLIST" -> {
                allow(function, p, "CUSTOMER_NUMBER", "SALES_ORGANIZATION", "PURCHASE_ORDER_NUMBER", "MATERIAL", "DOCUMENT_DATE");
                String po = get(p, "PURCHASE_ORDER_NUMBER", "");
                List<Map<String, String>> rows = new ArrayList<>();
                for (Order o : orders.values()) {
                    if (o.customer().equals(get(p, "CUSTOMER_NUMBER", "")) && o.salesOrg().equals(get(p, "SALES_ORGANIZATION", ""))
                            && (po.isEmpty() || o.purchaseOrder().equals(po))) {
                        rows.add(Map.of("SD_DOC", o.number(), "PURCH_NO_C", o.purchaseOrder(), "SOLD_TO", o.customer(), "DOC_DATE", o.date()));
                    }
                }
                out.put("RETURN", Map.of());
                out.put("SALES_ORDERS", rows);
            }
            case "BAPI_SALESORDER_GETSTATUS" -> {
                allow(function, p, "SALESDOCUMENT");
                Order o = orders.get(get(p, "SALESDOCUMENT", ""));
                if (o == null) {
                    out.put("RETURN", Map.of("TYPE", "E", "CODE", "V1302", "MESSAGE", "Sales document is not in the database"));
                    out.put("STATUSINFO", List.of());
                } else {
                    out.put("RETURN", Map.of());
                    out.put("STATUSINFO", List.of(Map.of("SD_DOC", o.number(), "PRC_STAT_H", o.delivered() ? "C" : "A",
                            "PURCH_NO", o.purchaseOrder())));
                }
            }
            case "BAPI_SALESORDER_CHANGE" -> {
                allow(function, p, "SALESDOCUMENT", "ORDER_HEADER_IN", "ORDER_HEADER_INX");
                String doc = get(p, "SALESDOCUMENT", "");
                Map<String, String> inx = get(p, "ORDER_HEADER_INX", Map.of());
                Order o = orders.get(doc);
                List<Map<String, String>> ret = new ArrayList<>();
                if (o == null) {
                    ret.add(msg("E", "V1", "302", "Sales document " + doc + " is not in the database"));
                } else if (!"D".equals(inx.get("UPDATEFLAG"))) {
                    ret.add(msg("E", "V4", "219", "Only deletion is supported by this fake"));
                } else if (o.delivered()) {
                    ret.add(msg("E", "V1", "084", "Sales document " + doc + " cannot be deleted: subsequent documents exist"));
                } else {
                    luw.add(() -> orders.remove(doc));
                    ret.add(msg("S", "V1", "041", "Standard Order " + doc + " has been deleted"));
                }
                out.put("RETURN", ret);
            }
            case "BAPI_CUSTOMER_GETDETAIL2" -> {
                allow(function, p, "CUSTOMERNO", "COMPANYCODE");
                Customer c = customers.get(get(p, "CUSTOMERNO", ""));
                if (c == null) {
                    out.put("RETURN", Map.of("TYPE", "E", "ID", "F2", "NUMBER", "153", "MESSAGE", "The customer does not exist"));
                    out.put("CUSTOMERADDRESS", Map.of());
                } else {
                    out.put("RETURN", Map.of());
                    out.put("CUSTOMERADDRESS", Map.of("NAME", c.name(), "STREET", c.street(), "POSTL_COD1", c.postalCode(),
                            "CITY", c.city(), "COUNTRY", c.country()));
                }
            }
            default -> throw new IllegalStateException("function module " + function + " is not in the repository");
        }
        return out;
    }

    private void create(Map<String, Object> p, List<Runnable> luw, Map<String, Object> out) {
        allow("BAPI_SALESORDER_CREATEFROMDAT2", p, "ORDER_HEADER_IN", "ORDER_PARTNERS", "ORDER_ITEMS_IN",
                "ORDER_SCHEDULES_IN", "TESTRUN");
        Map<String, String> h = get(p, "ORDER_HEADER_IN", Map.of());
        List<Map<String, String>> partners = get(p, "ORDER_PARTNERS", List.of());
        List<Map<String, String>> items = get(p, "ORDER_ITEMS_IN", List.of());
        List<Map<String, String>> ret = new ArrayList<>();
        String soldTo = partners.stream().filter(x -> "AG".equals(x.get("PARTN_ROLE"))).map(x -> x.get("PARTN_NUMB")).findFirst().orElse("");
        Customer c = customers.get(soldTo);
        if (!"1000".equals(h.get("SALES_ORG")) || !"10".equals(h.get("DISTR_CHAN")) || !"00".equals(h.get("DIVISION"))) {
            ret.add(msg("E", "V1", "312", "Sales area " + h.get("SALES_ORG") + "/" + h.get("DISTR_CHAN") + "/" + h.get("DIVISION") + " is not defined"));
        }
        if (c == null) {
            ret.add(msg("E", "V1", "390", "Sold-to party " + soldTo + " not maintained for sales area"));
        } else if (c.blocked()) {
            ret.add(msg("E", "V1", "462", "Sold-to party " + soldTo + " is blocked for sales"));
        }
        if (items.isEmpty()) {
            ret.add(msg("E", "V1", "261", "Enter at least one item"));
        }
        for (Map<String, String> it : items) {
            if (!materials.contains(it.get("MATERIAL"))) {
                ret.add(msg("E", "V1", "392", "Material " + it.get("MATERIAL") + " does not exist"));
            } else if (new BigDecimal(it.getOrDefault("TARGET_QTY", "0")).signum() <= 0) {
                ret.add(msg("E", "V1", "393", "Item " + it.get("ITM_NUMBER") + ": enter a quantity"));
            }
        }
        if (!ret.isEmpty()) {
            out.put("SALESDOCUMENT", "");
            out.put("RETURN", ret);
            return;
        }
        if ("X".equals(p.get("TESTRUN"))) {
            ret.add(msg("S", "V4", "233", "SALES_HEADER_IN has been processed successfully"));
            ret.add(msg("S", "V4", "233", "SALES_ITEM_IN has been processed successfully"));
            out.put("SALESDOCUMENT", "");
            out.put("RETURN", ret);
            return;
        }
        String number = String.format("%010d", next.getAndIncrement());
        String date = h.getOrDefault("REQ_DATE_H", LocalDate.now().toString().replace("-", ""));
        Order o = new Order(number, soldTo, h.get("SALES_ORG"), h.getOrDefault("PURCH_NO_C", ""), date, List.copyOf(items), false);
        luw.add(() -> orders.put(number, o));
        ret.add(msg("S", "V1", "311", "Standard Order " + number + " has been saved"));
        out.put("SALESDOCUMENT", number);
        out.put("RETURN", ret);
    }
}
