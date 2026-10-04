package com.sap.conn.jco;

public interface JCoFunction {
    JCoParameterList getImportParameterList();

    JCoParameterList getChangingParameterList();

    JCoParameterList getExportParameterList();

    JCoParameterList getTableParameterList();

    void execute(JCoDestination destination) throws JCoException;
}
