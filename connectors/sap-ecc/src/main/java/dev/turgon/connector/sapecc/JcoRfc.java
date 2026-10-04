package dev.turgon.connector.sapecc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RFC through SAP JCo 3, called by reflection: JCo is SAP's to license and
 * is not on the build's class path. Each endpoint instance is a JCo
 * destination served from memory (no properties files with passwords);
 * a rotated secret becomes a new destination.
 */
final class JcoRfc implements Rfc {
    private static final String P = "com.sap.conn.jco.";
    private static final Map<String, Properties> DESTINATIONS = new ConcurrentHashMap<>();
    private static final AtomicInteger SEQ = new AtomicInteger();
    private static volatile Object listener; // DestinationDataEventListener, once JCo hands it over
    private static boolean registered;

    private final String name;
    private final String where;
    private final Object destination;

    private JcoRfc(String name, String where, Object destination) {
        this.name = name;
        this.where = where;
        this.destination = destination;
    }

    static Rfc open(String endpoint, EccConfig config) throws Exception {
        register();
        Properties props = new Properties();
        props.putAll(config.destination());
        String name = "turgon-" + endpoint + "-" + SEQ.incrementAndGet();
        DESTINATIONS.put(name, props);
        Object dest = invokeStatic(P + "JCoDestinationManager", "getDestination", new Class<?>[] {String.class}, name);
        return new JcoRfc(name, config.where(), dest);
    }

    /** Registers the in-memory destination provider; JCo allows one per JVM. */
    private static synchronized void register() throws Exception {
        if (registered) {
            return;
        }
        ClassLoader cl = JcoRfc.class.getClassLoader();
        Class<?> provider = Class.forName(P + "ext.DestinationDataProvider", true, cl);
        Object proxy = Proxy.newProxyInstance(cl, new Class<?>[] {provider}, (self, m, args) -> switch (m.getName()) {
            case "getDestinationProperties" -> DESTINATIONS.get((String) args[0]);
            case "supportsEvents" -> true;
            case "setDestinationDataEventListener" -> {
                listener = args[0];
                yield null;
            }
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            case "toString" -> "turgon destinations";
            default -> null;
        });
        invokeStatic(P + "ext.Environment", "registerDestinationDataProvider", new Class<?>[] {provider}, proxy);
        registered = true;
    }

    private static Class<?> type(String name) throws ClassNotFoundException {
        return Class.forName(name, true, JcoRfc.class.getClassLoader());
    }

    private static Object invokeStatic(String cls, String method, Class<?>[] types, Object... args) throws Exception {
        return unwrap(() -> type(cls).getMethod(method, types).invoke(null, args));
    }

    /** Calls a method of a JCo interface on an object implementing it. */
    private static Object invoke(Object target, String iface, String method, Class<?>[] types, Object... args) throws Exception {
        Method m = type(P + iface).getMethod(method, types);
        return unwrap(() -> m.invoke(target, args));
    }

    private static Object invoke(Object target, String iface, String method) throws Exception {
        return invoke(target, iface, method, new Class<?>[0]);
    }

    @FunctionalInterface
    private interface Reflective {
        Object run() throws Exception;
    }

    private static Object unwrap(Reflective r) throws Exception {
        try {
            return r.run();
        } catch (InvocationTargetException e) {
            Throwable c = e.getCause();
            if (c != null && c.getClass().getName().equals(P + "AbapException")) {
                String key = (String) c.getClass().getMethod("getKey").invoke(c);
                throw new AbapError(key, c.getMessage());
            }
            if (c instanceof Exception x) {
                throw x;
            }
            throw e;
        }
    }

    @Override
    public Map<String, Object> call(String function, Map<String, Object> params) throws Exception {
        Object repo = invoke(destination, "JCoDestination", "getRepository");
        Object fn = invoke(repo, "JCoRepository", "getFunction", new Class<?>[] {String.class}, function);
        if (fn == null) {
            throw new IllegalStateException("function module " + function + " is not in the repository");
        }
        Object imports = invoke(fn, "JCoFunction", "getImportParameterList");
        Object changing = invoke(fn, "JCoFunction", "getChangingParameterList");
        Object tables = invoke(fn, "JCoFunction", "getTableParameterList");
        for (var p : params.entrySet()) {
            Object list = has(imports, p.getKey()) ? imports : has(changing, p.getKey()) ? changing : has(tables, p.getKey()) ? tables : null;
            if (list == null) {
                throw new IllegalArgumentException(function + " has no parameter " + p.getKey());
            }
            set(list, p.getKey(), p.getValue());
        }
        invoke(fn, "JCoFunction", "execute", new Class<?>[] {type(P + "JCoDestination")}, destination);
        Map<String, Object> out = new LinkedHashMap<>();
        for (String list : List.of("getExportParameterList", "getChangingParameterList", "getTableParameterList")) {
            Object l = invoke(fn, "JCoFunction", list);
            if (l != null) {
                out.putAll(read(l));
            }
        }
        return out;
    }

    private static boolean has(Object list, String name) throws Exception {
        if (list == null) {
            return false;
        }
        Object md = invoke(list, "JCoRecord", "getMetaData");
        return (boolean) invoke(md, "JCoMetaData", "hasField", new Class<?>[] {String.class}, name);
    }

    @SuppressWarnings("unchecked")
    private static void set(Object list, String name, Object value) throws Exception {
        Class<?>[] nv = {String.class, String.class};
        switch (value) {
            case String s -> invoke(list, "JCoRecord", "setValue", nv, name, s);
            case Map<?, ?> m -> {
                Object st = invoke(list, "JCoRecord", "getStructure", new Class<?>[] {String.class}, name);
                for (var f : ((Map<String, String>) m).entrySet()) {
                    invoke(st, "JCoRecord", "setValue", nv, f.getKey(), f.getValue());
                }
            }
            case List<?> rows -> {
                Object t = invoke(list, "JCoRecord", "getTable", new Class<?>[] {String.class}, name);
                for (Object row : rows) {
                    invoke(t, "JCoTable", "appendRow");
                    for (var f : ((Map<String, String>) row).entrySet()) {
                        invoke(t, "JCoRecord", "setValue", nv, f.getKey(), f.getValue());
                    }
                }
            }
            default -> throw new IllegalArgumentException(name + ": unsupported value " + value.getClass().getSimpleName());
        }
    }

    private static Map<String, Object> read(Object record) throws Exception {
        Object md = invoke(record, "JCoRecord", "getMetaData");
        int n = (int) invoke(md, "JCoMetaData", "getFieldCount");
        Class<?>[] i = {int.class};
        Map<String, Object> out = new LinkedHashMap<>();
        for (int f = 0; f < n; f++) {
            String name = (String) invoke(md, "JCoMetaData", "getName", i, f);
            if ((boolean) invoke(md, "JCoMetaData", "isTable", i, f)) {
                Object t = invoke(record, "JCoRecord", "getTable", i, f);
                int rows = (int) invoke(t, "JCoTable", "getNumRows");
                List<Map<String, String>> list = new ArrayList<>(rows);
                for (int r = 0; r < rows; r++) {
                    invoke(t, "JCoTable", "setRow", i, r);
                    list.add(flat(t));
                }
                out.put(name, list);
            } else if ((boolean) invoke(md, "JCoMetaData", "isStructure", i, f)) {
                out.put(name, flat(invoke(record, "JCoRecord", "getStructure", i, f)));
            } else {
                out.put(name, invoke(record, "JCoRecord", "getString", i, f));
            }
        }
        return out;
    }

    private static Map<String, String> flat(Object record) throws Exception {
        Object md = invoke(record, "JCoRecord", "getMetaData");
        int n = (int) invoke(md, "JCoMetaData", "getFieldCount");
        Class<?>[] i = {int.class};
        Map<String, String> out = new LinkedHashMap<>();
        for (int f = 0; f < n; f++) {
            out.put((String) invoke(md, "JCoMetaData", "getName", i, f), (String) invoke(record, "JCoRecord", "getString", i, f));
        }
        return out;
    }

    @Override
    public <T> T session(Work<T> work) throws Exception {
        Class<?>[] d = {type(P + "JCoDestination")};
        invokeStatic(P + "JCoContext", "begin", d, destination);
        try {
            return work.run(this);
        } finally {
            invokeStatic(P + "JCoContext", "end", d, destination);
        }
    }

    @Override
    public void ping() throws Exception {
        invoke(destination, "JCoDestination", "ping");
    }

    @Override
    public boolean exists(String function) throws Exception {
        Object repo = invoke(destination, "JCoDestination", "getRepository");
        return invoke(repo, "JCoRepository", "getFunction", new Class<?>[] {String.class}, function) != null;
    }

    @Override
    public String describe() {
        return "SAP JCo to " + where;
    }

    @Override
    public void close() throws Exception {
        DESTINATIONS.remove(name);
        Object l = listener;
        if (l != null) {
            invoke(l, "ext.DestinationDataEventListener", "deleted", new Class<?>[] {String.class}, name);
        }
    }
}
