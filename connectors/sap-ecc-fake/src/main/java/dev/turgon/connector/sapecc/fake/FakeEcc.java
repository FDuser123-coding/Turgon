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
 * cannot be deleted, and unknown parameters fail as in JCo. Saving an order
 * sends an order confirmation (ORDRSP, basic type ORDERS05) to the
 * registered server programs over "tRFC": a transaction they fail is sent
 * again every half second, as SAP's SM58 does.
 *
 * <p>Customers 1000 (Ada Lovelace GmbH), 1001 (Hopper Labs) and 1002
 * (blocked); materials M-1, M-7, M-8 and M-9; sales area 1000/10/00.
 */
public final class FakeEcc {
    private static final Map<String, FakeEcc> SYSTEMS = new ConcurrentHashMap<>();
    static final Set<String> FUNCTIONS = Set.of("RFC_PING", "BAPI_SALESORDER_CREATEFROMDAT2", "BAPI_SALESORDER_GETLIST",
            "BAPI_SALESORDER_GETSTATUS", "BAPI_SALESORDER_CHANGE", "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK",
            "BAPI_CUSTOMER_GETDETAIL2", "IDOCTYPE_READ_COMPLETE", "RFC_GET_FUNCTION_INTERFACE", "DDIF_FIELDINFO_GET");

    /** Dictionary structures: field -> {type, length, text}. Tests change them to see drift. */
    public final Map<String, Map<String, Object[]>> structures = new ConcurrentHashMap<>();

    /** The BAPIs' parameters: {class, name, structure or type, optional}. */
    static final Map<String, List<String[]>> INTERFACES = Map.of(
            "BAPI_SALESORDER_CREATEFROMDAT2", List.<String[]>of(new String[] {"I", "ORDER_HEADER_IN", "BAPISDHD1", ""},
                    new String[] {"I", "TESTRUN", "BAPIFLAG-BAPIFLAG", "X"}, new String[] {"E", "SALESDOCUMENT", "BAPIVBELN-VBELN", ""},
                    new String[] {"T", "RETURN", "BAPIRET2", "X"}, new String[] {"T", "ORDER_ITEMS_IN", "BAPISDITM", "X"},
                    new String[] {"T", "ORDER_PARTNERS", "BAPIPARNR", ""}, new String[] {"T", "ORDER_SCHEDULES_IN", "BAPISCHDL", "X"}),
            "BAPI_SALESORDER_GETLIST", List.<String[]>of(new String[] {"I", "CUSTOMER_NUMBER", "BAPI1007-CUSTOMER", ""},
                    new String[] {"I", "SALES_ORGANIZATION", "VBAK-VKORG", ""}, new String[] {"I", "PURCHASE_ORDER_NUMBER", "VBKD-BSTKD", "X"},
                    new String[] {"E", "RETURN", "BAPIRETURN", ""}, new String[] {"T", "SALES_ORDERS", "BAPIORDERS", ""}),
            "BAPI_SALESORDER_GETSTATUS", List.<String[]>of(new String[] {"I", "SALESDOCUMENT", "BAPIVBELN-VBELN", ""},
                    new String[] {"E", "RETURN", "BAPIRETURN", ""}, new String[] {"T", "STATUSINFO", "BAPISDSTAT", ""}),
            "BAPI_SALESORDER_CHANGE", List.<String[]>of(new String[] {"I", "SALESDOCUMENT", "BAPIVBELN-VBELN", ""},
                    new String[] {"I", "ORDER_HEADER_INX", "BAPISDH1X", ""}, new String[] {"T", "RETURN", "BAPIRET2", ""}),
            "BAPI_TRANSACTION_COMMIT", List.<String[]>of(new String[] {"I", "WAIT", "BAPITA-WAIT", "X"}, new String[] {"E", "RETURN", "BAPIRET2", ""}),
            "BAPI_TRANSACTION_ROLLBACK", List.<String[]>of(new String[] {"E", "RETURN", "BAPIRET2", ""}),
            "BAPI_CUSTOMER_GETDETAIL2", List.<String[]>of(new String[] {"I", "CUSTOMERNO", "BAPI1007-CUSTOMER", ""},
                    new String[] {"E", "CUSTOMERADDRESS", "BAPICUSTOMER_04", ""}, new String[] {"E", "RETURN", "BAPIRETURN1", ""}));

    private static Map<String, Object[]> structure(Object... f) {
        Map<String, Object[]> m = new LinkedHashMap<>();
        for (int i = 0; i < f.length; i += 4) {
            m.put((String) f[i], new Object[] {f[i + 1], f[i + 2], f[i + 3]});
        }
        return m;
    }

    public record Customer(String name, String street, String postalCode, String city, String country, boolean blocked) {}

    public record Order(String number, String customer, String salesOrg, String purchaseOrder, String date,
            List<Map<String, String>> items, boolean delivered) {}

    private final Map<String, Customer> customers = new ConcurrentHashMap<>();
    private final Set<String> materials = ConcurrentHashMap.newKeySet();
    private final Map<String, Order> orders = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger(12000);
    private final Map<String, Integer> failures = new ConcurrentHashMap<>();
    private final Map<String, dev.turgon.connector.sapecc.IdocReceiver> programs = new ConcurrentHashMap<>();
    private final java.util.concurrent.LinkedBlockingDeque<Transaction> outbound = new java.util.concurrent.LinkedBlockingDeque<>();
    private final AtomicInteger docnums = new AtomicInteger(4711);
    /** Transactions SAP's partners accepted, in order, for tests. */
    public final List<Transaction> sent = java.util.Collections.synchronizedList(new ArrayList<>());
    private Thread sender;

    /** One tRFC transaction of IDocs, and how often it was tried. */
    public static final class Transaction {
        public final String tid;
        public final List<Map<String, String>> control;
        public final List<Map<String, String>> data;
        public volatile int attempts;
        public volatile String lastError = "";

        Transaction(String tid, List<Map<String, String>> control, List<Map<String, String>> data) {
            this.tid = tid;
            this.control = control;
            this.data = data;
        }
    }

    /** Registers a server program; IDocs go to every registered one. */
    public synchronized AutoCloseable register(String progid, dev.turgon.connector.sapecc.IdocReceiver receiver) {
        programs.put(progid, receiver);
        if (sender == null) {
            sender = new Thread(this::send, "fake-ecc-trfc");
            sender.setDaemon(true);
            sender.start();
        }
        return () -> programs.remove(progid, receiver);
    }

    /** Queues IDocs for the registered programs (outbound processing). */
    public void send(List<Map<String, String>> control, List<Map<String, String>> data) {
        outbound.add(new Transaction(java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 24).toUpperCase(), control, data));
    }

    /** The transactions not yet accepted (SM58). */
    public List<Transaction> pending() {
        return List.copyOf(outbound);
    }

    private void send() {
        while (true) {
            Transaction t;
            try {
                t = outbound.takeFirst();
            } catch (InterruptedException e) {
                return;
            }
            var receivers = List.copyOf(programs.values());
            t.attempts++;
            try {
                if (receivers.isEmpty()) {
                    throw new IllegalStateException("no server program is registered");
                }
                receivers.getFirst().receive(t.tid, t.control, t.data);
                sent.add(t);
            } catch (Exception e) {
                t.lastError = e.getMessage();
                outbound.addFirst(t); // stays first: tRFC keeps the order
                try {
                    Thread.sleep(500);
                } catch (InterruptedException x) {
                    return;
                }
            }
        }
    }

    /** ORDERS05's segments, the fields the order confirmation fills (offsets as SAP gives them). */
    static final Map<String, List<Object[]>> ORDERS05 = new LinkedHashMap<>();

    static {
        ORDERS05.put("E1EDK01", List.of(new Object[] {"ACTION", 3}, new Object[] {"KZABS", 1}, new Object[] {"CURCY", 3},
                new Object[] {"HWAER", 3}, new Object[] {"WKURS", 12}, new Object[] {"ZTERM", 17}, new Object[] {"BSART", 4},
                new Object[] {"BELNR", 35}));
        ORDERS05.put("E1EDKA1", List.of(new Object[] {"PARVW", 3}, new Object[] {"PARTN", 17}, new Object[] {"LIFNR", 17},
                new Object[] {"NAME1", 35}));
        ORDERS05.put("E1EDK02", List.of(new Object[] {"QUALF", 3}, new Object[] {"BELNR", 35}, new Object[] {"POSNR", 6},
                new Object[] {"DATUM", 8}));
        ORDERS05.put("E1EDP01", List.of(new Object[] {"POSEX", 6}, new Object[] {"ACTION", 3}, new Object[] {"PSTYP", 1},
                new Object[] {"KZABS", 1}, new Object[] {"MENGE", 15}, new Object[] {"MENEE", 3}));
        ORDERS05.put("E1EDP19", List.of(new Object[] {"QUALF", 3}, new Object[] {"IDTNR", 35}, new Object[] {"KTEXT", 70}));
        ORDERS05.put("E1EDS01", List.of(new Object[] {"SUMID", 3}, new Object[] {"SUMME", 18}, new Object[] {"SUNIT", 3},
                new Object[] {"WAERQ", 3}));
    }

    /** A data record: the segment's fields laid out in SDATA. */
    static Map<String, String> record(String docnum, int segnum, int parent, int level, String segment, Map<String, String> values) {
        StringBuilder sdata = new StringBuilder();
        for (Object[] f : ORDERS05.get(segment)) {
            String v = values.getOrDefault((String) f[0], "");
            int len = (Integer) f[1];
            sdata.append(String.format("%-" + len + "s", v.length() > len ? v.substring(0, len) : v));
        }
        return Map.of("SEGNAM", segment, "DOCNUM", docnum, "SEGNUM", String.format("%06d", segnum),
                "PSGNUM", String.format("%06d", parent), "HLEVEL", String.format("%02d", level), "SDATA", sdata.toString());
    }

    /** Net prices per unit, in EUR. */
    static final Map<String, BigDecimal> PRICES = Map.of("M-1", new BigDecimal("100.00"), "M-7", new BigDecimal("49.90"),
            "M-8", new BigDecimal("12.50"), "M-9", new BigDecimal("7.00"));

    /** The order confirmation SAP's output determination (BA00) sends when an order is saved. */
    void confirmOrder(Order o) {
        String docnum = String.format("%016d", docnums.getAndIncrement());
        Map<String, String> control = Map.ofEntries(Map.entry("DOCNUM", docnum), Map.entry("IDOCTYP", "ORDERS05"),
                Map.entry("CIMTYP", ""), Map.entry("MESTYP", "ORDRSP"), Map.entry("SNDPRT", "LS"), Map.entry("SNDPRN", "ECCCLNT100"),
                Map.entry("RCVPRT", "LS"), Map.entry("RCVPRN", "TURGON"), Map.entry("CREDAT", o.date()), Map.entry("CRETIM", "120000"));
        List<Map<String, String>> data = new ArrayList<>();
        int seg = 0;
        Customer c = customers.get(o.customer());
        data.add(record(docnum, ++seg, 0, 1, "E1EDK01", Map.of("CURCY", "EUR", "BSART", "TA", "BELNR", o.number())));
        data.add(record(docnum, ++seg, 0, 2, "E1EDKA1", Map.of("PARVW", "AG", "PARTN", o.customer(), "NAME1", c == null ? "" : c.name())));
        data.add(record(docnum, ++seg, 0, 2, "E1EDK02", Map.of("QUALF", "001", "BELNR", o.purchaseOrder(), "DATUM", o.date())));
        for (Map<String, String> it : o.items()) {
            int item = ++seg;
            data.add(record(docnum, item, 0, 2, "E1EDP01", Map.of("POSEX", it.getOrDefault("ITM_NUMBER", ""),
                    "MENGE", it.getOrDefault("TARGET_QTY", ""), "MENEE", "ST")));
            data.add(record(docnum, ++seg, item, 3, "E1EDP19", Map.of("QUALF", "002", "IDTNR", it.getOrDefault("MATERIAL", ""))));
        }
        BigDecimal net = BigDecimal.ZERO;
        for (Map<String, String> it : o.items()) {
            net = net.add(PRICES.getOrDefault(it.get("MATERIAL"), BigDecimal.ZERO).multiply(new BigDecimal(it.getOrDefault("TARGET_QTY", "0"))));
        }
        data.add(record(docnum, ++seg, 0, 1, "E1EDS01", Map.of("SUMID", "002", "SUMME", net.setScale(2, java.math.RoundingMode.HALF_UP).toPlainString(),
                "WAERQ", "EUR")));
        send(List.of(control), data);
    }
    /** Calls made, by function, for tests. */
    public final List<String> calls = java.util.Collections.synchronizedList(new ArrayList<>());

    public FakeEcc() {
        customers.put("0000001000", new Customer("Ada Lovelace GmbH", "Unter den Linden 1", "10117", "Berlin", "DE", false));
        customers.put("0000001001", new Customer("Hopper Labs", "1 Navy Way", "20301", "Arlington", "US", false));
        customers.put("0000001002", new Customer("Blocked Trading Ltd", "1 Main St", "EC1A", "London", "GB", true));
        materials.addAll(List.of("M-1", "M-7", "M-8", "M-9"));
        structures.put("BAPISDHD1", structure("DOC_TYPE", "CHAR", 4, "Sales Document Type", "SALES_ORG", "CHAR", 4, "Sales Organization",
                "DISTR_CHAN", "CHAR", 2, "Distribution Channel", "DIVISION", "CHAR", 2, "Division",
                "PURCH_NO_C", "CHAR", 35, "Customer purchase order number", "REQ_DATE_H", "DATS", 8, "Requested delivery date",
                "CURRENCY", "CUKY", 5, "Currency"));
        structures.put("BAPISDITM", structure("ITM_NUMBER", "NUMC", 6, "Item number", "MATERIAL", "CHAR", 18, "Material",
                "TARGET_QTY", "QUAN", 13, "Target quantity"));
        structures.put("BAPIPARNR", structure("PARTN_ROLE", "CHAR", 2, "Partner function", "PARTN_NUMB", "CHAR", 10, "Customer number"));
        structures.put("BAPISCHDL", structure("ITM_NUMBER", "NUMC", 6, "Item number", "REQ_QTY", "QUAN", 13, "Order quantity"));
        structures.put("BAPISDH1X", structure("UPDATEFLAG", "CHAR", 1, "Update indicator"));
        structures.put("BAPICUSTOMER_04", structure("NAME", "CHAR", 35, "Name", "STREET", "CHAR", 35, "Street",
                "POSTL_COD1", "CHAR", 10, "Postal code", "CITY", "CHAR", 35, "City", "COUNTRY", "CHAR", 3, "Country"));
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
            case "RFC_GET_FUNCTION_INTERFACE" -> {
                allow(function, p, "FUNCNAME", "LANGUAGE", "NONE_UNICODE_LENGTH");
                List<String[]> params = INTERFACES.get(get(p, "FUNCNAME", ""));
                if (params == null) {
                    throw new IllegalStateException("ABAP exception FU_NOT_FOUND");
                }
                List<Map<String, String>> rows = new ArrayList<>();
                for (String[] q : params) {
                    rows.add(Map.of("PARAMCLASS", q[0], "PARAMETER", q[1], "TABNAME", q[2].contains("-") ? "" : q[2],
                            "EXID", q[2].contains("-") ? "C" : "u", "OPTIONAL", q[3], "FIELDNAME", "", "PARAMTEXT", q[1]));
                }
                out.put("PARAMS", rows);
            }
            case "DDIF_FIELDINFO_GET" -> {
                allow(function, p, "TABNAME", "LANGU", "ALL_TYPES");
                Map<String, Object[]> st = structures.get(get(p, "TABNAME", ""));
                List<Map<String, String>> rows = new ArrayList<>();
                if (st != null) {
                    st.forEach((f, d) -> rows.add(Map.of("FIELDNAME", f, "DATATYPE", (String) d[0], "LENG", String.format("%06d", (Integer) d[1]),
                            "FIELDTEXT", (String) d[2], "KEYFLAG", "")));
                }
                out.put("DFIES_TAB", rows);
            }
            case "IDOCTYPE_READ_COMPLETE" -> {
                allow(function, p, "PI_IDOCTYP", "PI_CIMTYP", "PI_RELEASE", "PI_APPLREL");
                List<Map<String, String>> segments = new ArrayList<>(), fields = new ArrayList<>();
                if ("ORDERS05".equals(get(p, "PI_IDOCTYP", ""))) {
                    int nr = 0;
                    for (var seg : ORDERS05.entrySet()) {
                        segments.add(Map.of("SEGMENTTYP", seg.getKey(), "NR", String.format("%04d", ++nr)));
                        int at = 64; // SDATA starts after the data record's 63-byte header
                        for (Object[] f : seg.getValue()) {
                            fields.add(Map.of("SEGMENTTYP", seg.getKey(), "FIELDNAME", (String) f[0], "DATATYPE", "CHAR",
                                    "BYTE_FIRST", String.format("%06d", at), "EXTLEN", String.format("%06d", (Integer) f[1])));
                            at += (Integer) f[1];
                        }
                    }
                }
                out.put("PT_SEGMENTS", segments);
                out.put("PT_FIELDS", fields);
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
        luw.add(() -> {
            orders.put(number, o);
            confirmOrder(o);
        });
        ret.add(msg("S", "V1", "311", "Standard Order " + number + " has been saved"));
        out.put("SALESDOCUMENT", number);
        out.put("RETURN", ret);
    }
}
