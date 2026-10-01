package com.sap.conn.jco;

public interface JCoDestination {
    void ping() throws JCoException;

    JCoRepository getRepository() throws JCoException;
}
