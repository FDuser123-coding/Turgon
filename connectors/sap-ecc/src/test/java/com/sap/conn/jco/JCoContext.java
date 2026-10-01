package com.sap.conn.jco;

public final class JCoContext {
    private JCoContext() {}

    public static void begin(JCoDestination d) {
        ((JCoDestinationManager.Dest) d).conn.begin();
    }

    public static void end(JCoDestination d) {
        ((JCoDestinationManager.Dest) d).conn.end();
    }
}
