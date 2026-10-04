package com.sap.conn.jco.server;

public interface JCoServer {
    void setCallHandlerFactory(JCoServerFunctionHandlerFactory factory);

    void setTIDHandler(JCoServerTIDHandler handler);

    void start();

    void stop();

    void release();
}
