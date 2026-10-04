package dev.turgon.connector.sapecc;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A JCo server program registered at the SAP gateway, receiving IDocs over
 * tRFC (IDOC_INBOUND_ASYNCHRONOUS), called by reflection like the rest of
 * JCo. SAP's partner profile sends to an RFC destination of type T that
 * names this program ID. Every transaction is accepted (checkTID): the
 * worker drops IDocs it already has, so a transaction SAP repeats is
 * harmless, and nothing is kept here.
 */
final class JcoIdocServer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(JcoIdocServer.class.getName());
    private static final String P = "com.sap.conn.jco.";
    private static final Map<String, Properties> SERVERS = new ConcurrentHashMap<>();
    private static final AtomicInteger SEQ = new AtomicInteger();
    private static boolean registered;

    private final String name;
    private final Object server;

    private JcoIdocServer(String name, Object server) {
        this.name = name;
        this.server = server;
    }

    private static synchronized void register() throws Exception {
        if (registered) {
            return;
        }
        ClassLoader cl = JcoIdocServer.class.getClassLoader();
        Class<?> provider = JcoRfc.type(P + "ext.ServerDataProvider");
        Object proxy = Proxy.newProxyInstance(cl, new Class<?>[] {provider}, (self, m, args) -> switch (m.getName()) {
            case "getServerProperties" -> SERVERS.get((String) args[0]);
            case "supportsEvents" -> false;
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            case "toString" -> "turgon servers";
            default -> null;
        });
        JcoRfc.invokeStatic(P + "ext.Environment", "registerServerDataProvider", new Class<?>[] {provider}, proxy);
        registered = true;
    }

    static JcoIdocServer start(String endpoint, EccConfig cfg, JcoRfc rfc, IdocReceiver receiver) throws Exception {
        register();
        Properties props = new Properties();
        props.putAll(cfg.server());
        props.setProperty("jco.server.repository_destination", rfc.destinationName());
        String name = "turgon-" + endpoint + "-idoc-" + SEQ.incrementAndGet();
        SERVERS.put(name, props);
        ClassLoader cl = JcoIdocServer.class.getClassLoader();
        Object server = JcoRfc.invokeStatic(P + "server.JCoServerFactory", "getServer", new Class<?>[] {String.class}, name);

        Class<?> handlerType = JcoRfc.type(P + "server.JCoServerFunctionHandler");
        Object handler = Proxy.newProxyInstance(cl, new Class<?>[] {handlerType}, (self, m, args) -> switch (m.getName()) {
            case "handleRequest" -> {
                Object ctx = args[0], fn = args[1];
                String tid = String.valueOf(JcoRfc.invoke(ctx, "server.JCoServerContext", "getTID"));
                Object tables = JcoRfc.invoke(fn, "JCoFunction", "getTableParameterList");
                Map<String, Object> t = JcoRfc.read(tables);
                try {
                    receiver.receive(tid, rows(t.get("IDOC_CONTROL_REC_40")), rows(t.get("IDOC_DATA_REC_40")));
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    // handleRequest declares only ABAP exceptions: anything else
                    // reaches SAP as a system failure, and SAP sends it again.
                    throw new IllegalStateException(e.getMessage(), e);
                }
                yield null;
            }
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            case "toString" -> "turgon IDoc handler";
            default -> null;
        });
        Object factory = JcoRfc.type(P + "server.DefaultServerHandlerFactory$FunctionHandlerFactory").getConstructor().newInstance();
        JcoRfc.invoke(factory, "server.DefaultServerHandlerFactory$FunctionHandlerFactory", "registerHandler",
                new Class<?>[] {String.class, handlerType}, "IDOC_INBOUND_ASYNCHRONOUS", handler);
        JcoRfc.invoke(server, "server.JCoServer", "setCallHandlerFactory",
                new Class<?>[] {JcoRfc.type(P + "server.JCoServerFunctionHandlerFactory")}, factory);

        Class<?> tidType = JcoRfc.type(P + "server.JCoServerTIDHandler");
        Object tids = Proxy.newProxyInstance(cl, new Class<?>[] {tidType}, (self, m, args) -> switch (m.getName()) {
            case "checkTID" -> true;
            case "hashCode" -> System.identityHashCode(self);
            case "equals" -> self == args[0];
            case "toString" -> "turgon TIDs";
            default -> null; // commit, rollback, confirmTID: the worker's inbox keeps what counts
        });
        JcoRfc.invoke(server, "server.JCoServer", "setTIDHandler", new Class<?>[] {tidType}, tids);
        JcoRfc.invoke(server, "server.JCoServer", "start");
        LOG.info(() -> "registered server program " + props.getProperty("jco.server.progid") + " at "
                + props.getProperty("jco.server.gwhost") + " " + props.getProperty("jco.server.gwserv"));
        return new JcoIdocServer(name, server);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, String>> rows(Object v) {
        return v instanceof List<?> l ? (List<Map<String, String>>) l : List.of();
    }

    @Override
    public void close() {
        try {
            JcoRfc.invoke(server, "server.JCoServer", "stop");
            JcoRfc.invoke(server, "server.JCoServer", "release");
        } catch (Exception e) {
            LOG.log(Level.WARNING, "stopping the server program failed", e);
        } finally {
            SERVERS.remove(name);
        }
    }
}
