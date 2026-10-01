package com.sap.conn.jco;

import com.sap.conn.jco.ext.Environment;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class JCoDestinationManager {
    private JCoDestinationManager() {}

    /** The destinations handed out, by name, with their properties. */
    public static final Map<String, Properties> SEEN = new LinkedHashMap<>();

    public static JCoDestination getDestination(String name) throws JCoException {
        Properties p = Environment.provider == null ? null : Environment.provider.getDestinationProperties(name);
        if (p == null) {
            throw new JCoException("destination " + name + " does not exist");
        }
        SEEN.put(name, p);
        return new Dest(FakeEcc.system(p.getProperty("jco.client.ashost")).connect(p.getProperty("jco.client.user")));
    }

    // name -> {imports, exports, tables}; a name in brackets is a structure.
    static final Map<String, List<Set<String>>> SIGNATURES = Map.of(
            "BAPI_SALESORDER_CREATEFROMDAT2", List.of(Set.of("[ORDER_HEADER_IN]", "TESTRUN"), Set.of("SALESDOCUMENT"),
                    Set.of("RETURN", "ORDER_PARTNERS", "ORDER_ITEMS_IN", "ORDER_SCHEDULES_IN")),
            "BAPI_SALESORDER_GETLIST", List.of(Set.of("CUSTOMER_NUMBER", "SALES_ORGANIZATION", "PURCHASE_ORDER_NUMBER"),
                    Set.of("[RETURN]"), Set.of("SALES_ORDERS")),
            "BAPI_SALESORDER_GETSTATUS", List.of(Set.of("SALESDOCUMENT"), Set.of("[RETURN]"), Set.of("STATUSINFO")),
            "BAPI_SALESORDER_CHANGE", List.of(Set.of("SALESDOCUMENT", "[ORDER_HEADER_INX]"), Set.of(), Set.of("RETURN")),
            "BAPI_TRANSACTION_COMMIT", List.of(Set.of("WAIT"), Set.of("[RETURN]"), Set.of()),
            "BAPI_TRANSACTION_ROLLBACK", List.of(Set.of(), Set.of("[RETURN]"), Set.of()),
            "BAPI_CUSTOMER_GETDETAIL2", List.of(Set.of("CUSTOMERNO"), Set.of("[CUSTOMERADDRESS]", "[RETURN]"), Set.of()),
            "RFC_PING", List.of(Set.of(), Set.of(), Set.of()),
            "Z_RAISE", List.of(Set.of(), Set.of(), Set.of()));

    static final class Dest implements JCoDestination {
        final FakeEcc.Connection conn;

        Dest(FakeEcc.Connection conn) {
            this.conn = conn;
        }

        @Override
        public void ping() throws JCoException {
            try {
                conn.ping();
            } catch (Exception e) {
                throw new JCoException(e.getMessage());
            }
        }

        @Override
        public JCoRepository getRepository() {
            return name -> SIGNATURES.containsKey(name) ? new Function(name, SIGNATURES.get(name)) : null;
        }
    }

    /** A record whose fields are declared (scalars, [structures], tables) or, for structures, open. */
    static class Rec implements JCoParameterList, JCoStructure {
        final Map<String, Object> values = new LinkedHashMap<>();
        final Set<String> declared;

        Rec(Set<String> declared) {
            this.declared = declared;
            if (declared != null) {
                for (String d : declared) {
                    if (d.startsWith("[")) {
                        values.put(d.substring(1, d.length() - 1), new Rec(null));
                    }
                }
            }
        }

        boolean isTable(String name) {
            return values.get(name) instanceof Tab;
        }

        @Override
        public JCoMetaData getMetaData() {
            List<String> names = new ArrayList<>(values.keySet());
            return new JCoMetaData() {
                @Override
                public boolean hasField(String n) {
                    return values.containsKey(n) || (declared != null && declared.contains(n));
                }

                @Override
                public int getFieldCount() {
                    return names.size();
                }

                @Override
                public String getName(int i) {
                    return names.get(i);
                }

                @Override
                public boolean isTable(int i) {
                    return values.get(names.get(i)) instanceof Tab;
                }

                @Override
                public boolean isStructure(int i) {
                    Object v = values.get(names.get(i));
                    return v instanceof Rec && !(v instanceof Tab);
                }
            };
        }

        @Override
        public void setValue(String name, String value) {
            values.put(name, value);
        }

        @Override
        public JCoStructure getStructure(String name) {
            return (JCoStructure) values.computeIfAbsent(name, n -> new Rec(null));
        }

        @Override
        public JCoTable getTable(String name) {
            return (JCoTable) values.computeIfAbsent(name, n -> new Tab());
        }

        @Override
        public JCoStructure getStructure(int i) {
            return (JCoStructure) values.values().toArray()[i];
        }

        @Override
        public JCoTable getTable(int i) {
            return (JCoTable) values.values().toArray()[i];
        }

        @Override
        public String getString(int i) {
            return String.valueOf(values.values().toArray()[i]);
        }
    }

    static final class Tab extends Rec implements JCoTable {
        final List<Map<String, String>> rows = new ArrayList<>();
        int row = -1;

        Tab() {
            super(null);
        }

        @Override
        public void appendRow() {
            rows.add(new LinkedHashMap<>());
            row = rows.size() - 1;
        }

        @Override
        public int getNumRows() {
            return rows.size();
        }

        @Override
        public void setRow(int r) {
            row = r;
        }

        @Override
        public void setValue(String name, String value) {
            rows.get(row).put(name, value);
        }

        @Override
        public JCoMetaData getMetaData() {
            List<String> names = new ArrayList<>(rows.get(row).keySet());
            return new JCoMetaData() {
                @Override
                public boolean hasField(String n) {
                    return names.contains(n);
                }

                @Override
                public int getFieldCount() {
                    return names.size();
                }

                @Override
                public String getName(int i) {
                    return names.get(i);
                }

                @Override
                public boolean isTable(int i) {
                    return false;
                }

                @Override
                public boolean isStructure(int i) {
                    return false;
                }
            };
        }

        @Override
        public String getString(int i) {
            return rows.get(row).values().toArray(new String[0])[i];
        }
    }

    static final class Function implements JCoFunction {
        final String name;
        final Rec imports;
        final Rec exports;
        final Rec tables;

        Function(String name, List<Set<String>> sig) {
            this.name = name;
            imports = new Rec(sig.get(0));
            exports = new Rec(sig.get(1));
            tables = new Rec(Set.of());
            for (String t : sig.get(2)) {
                tables.values.put(t, new Tab());
            }
        }

        @Override
        public JCoParameterList getImportParameterList() {
            return imports.declared.isEmpty() ? null : imports;
        }

        @Override
        public JCoParameterList getChangingParameterList() {
            return null;
        }

        @Override
        public JCoParameterList getExportParameterList() {
            return exports.declared.isEmpty() ? null : exports;
        }

        @Override
        public JCoParameterList getTableParameterList() {
            return tables.values.isEmpty() ? null : tables;
        }

        @Override
        public void execute(JCoDestination d) throws JCoException {
            if (name.equals("Z_RAISE")) {
                throw new AbapException("NOT_AUTHORIZED", "No authorization for the sales organization");
            }
            Map<String, Object> params = new LinkedHashMap<>();
            imports.values.forEach((k, v) -> {
                if (v instanceof Rec r && !(v instanceof Tab)) {
                    if (!r.values.isEmpty()) {
                        params.put(k, Map.copyOf(r.values.entrySet().stream()
                                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())))));
                    }
                } else {
                    params.put(k, v);
                }
            });
            tables.values.forEach((k, v) -> {
                Tab t = (Tab) v;
                if (!t.rows.isEmpty()) {
                    params.put(k, List.copyOf(t.rows));
                }
                t.rows.clear();
            });
            Map<String, Object> out;
            try {
                out = ((JCoDestinationManager.Dest) d).conn.call(name, params);
            } catch (Exception e) {
                throw new JCoException(e.getMessage());
            }
            for (var e : out.entrySet()) {
                if (tables.values.containsKey(e.getKey())) {
                    Tab t = (Tab) tables.values.get(e.getKey());
                    @SuppressWarnings("unchecked")
                    List<Map<String, String>> rows = e.getValue() instanceof List<?> l ? (List<Map<String, String>>) l : List.of();
                    rows.forEach(r -> t.rows.add(new LinkedHashMap<>(r)));
                } else if (exports.values.get(e.getKey()) instanceof Rec r && !(r instanceof Tab)) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> m = (Map<String, String>) e.getValue();
                    r.values.putAll(m);
                } else if (exports.declared.contains(e.getKey())) {
                    exports.values.put(e.getKey(), String.valueOf(e.getValue()));
                }
            }
        }
    }
}
