package com.sap.conn.jco;

public interface JCoRepository {
    JCoFunction getFunction(String name) throws JCoException;
}
