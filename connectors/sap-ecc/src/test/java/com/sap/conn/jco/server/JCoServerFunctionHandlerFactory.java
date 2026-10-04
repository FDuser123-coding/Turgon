package com.sap.conn.jco.server;

public interface JCoServerFunctionHandlerFactory {
    JCoServerFunctionHandler getCallHandler(JCoServerContext context, String functionName);
}
