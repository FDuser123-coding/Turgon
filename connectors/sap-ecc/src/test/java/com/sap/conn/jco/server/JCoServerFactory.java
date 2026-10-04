package com.sap.conn.jco.server;

import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.ext.Environment;
import dev.turgon.connector.sapecc.fake.FakeEcc;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** The stand-in's server: registers with the fake ECC its gateway host names and calls the handlers as JCo would. */
public final class JCoServerFactory {
    private JCoServerFactory() {}

    /** Servers by state, for tests. */
    public static final List<String> EVENTS = new ArrayList<>();

    public static JCoServer getServer(String name) {
        Properties p = Environment.serverProvider.getServerProperties(name);
        if (p == null) {
            throw new IllegalStateException("server " + name + " is not configured");
        }
        if (!JCoDestinationManager.SEEN.containsKey(p.getProperty("jco.server.repository_destination"))) {
            throw new IllegalStateException("the repository destination is unknown");
        }
        return new JCoServer() {
            JCoServerFunctionHandlerFactory factory;
            JCoServerTIDHandler tids;
            AutoCloseable registration;

            @Override
            public void setCallHandlerFactory(JCoServerFunctionHandlerFactory f) {
                factory = f;
            }

            @Override
            public void setTIDHandler(JCoServerTIDHandler h) {
                tids = h;
            }

            @Override
            public void start() {
                EVENTS.add("start " + p.getProperty("jco.server.progid"));
                registration = FakeEcc.system(p.getProperty("jco.server.gwhost")).register(p.getProperty("jco.server.progid"), (tid, control, data) -> {
                    JCoServerContext ctx = () -> tid;
                    if (!tids.checkTID(ctx, tid)) {
                        return;
                    }
                    var fn = new JCoDestinationManager.Function("IDOC_INBOUND_ASYNCHRONOUS",
                            JCoDestinationManager.SIGNATURES.get("IDOC_INBOUND_ASYNCHRONOUS"));
                    fill((JCoDestinationManager.Tab) fn.tables.values.get("IDOC_CONTROL_REC_40"), control);
                    fill((JCoDestinationManager.Tab) fn.tables.values.get("IDOC_DATA_REC_40"), data);
                    try {
                        factory.getCallHandler(ctx, "IDOC_INBOUND_ASYNCHRONOUS").handleRequest(ctx, fn);
                    } catch (RuntimeException e) {
                        tids.rollback(ctx, tid);
                        throw e;
                    }
                    tids.commit(ctx, tid);
                    tids.confirmTID(ctx, tid);
                });
            }

            private void fill(JCoDestinationManager.Tab t, List<Map<String, String>> rows) {
                for (Map<String, String> r : rows) {
                    t.appendRow();
                    r.forEach(t::setValue);
                }
            }

            @Override
            public void stop() {
                EVENTS.add("stop " + p.getProperty("jco.server.progid"));
                try {
                    registration.close();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void release() {
                EVENTS.add("release");
            }
        };
    }
}
