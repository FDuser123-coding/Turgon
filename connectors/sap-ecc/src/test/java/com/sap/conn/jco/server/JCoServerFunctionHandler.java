package com.sap.conn.jco.server;

import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoFunction;

public interface JCoServerFunctionHandler {
    void handleRequest(JCoServerContext context, JCoFunction function) throws AbapException;
}
