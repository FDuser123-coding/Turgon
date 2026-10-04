package com.sap.conn.jco.server;

public interface JCoServerTIDHandler {
    boolean checkTID(JCoServerContext context, String tid);

    void confirmTID(JCoServerContext context, String tid);

    void commit(JCoServerContext context, String tid);

    void rollback(JCoServerContext context, String tid);
}
