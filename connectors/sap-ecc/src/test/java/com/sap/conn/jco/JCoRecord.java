package com.sap.conn.jco;

public interface JCoRecord {
    JCoMetaData getMetaData();

    void setValue(String name, String value);

    JCoStructure getStructure(String name);

    JCoTable getTable(String name);

    JCoStructure getStructure(int index);

    JCoTable getTable(int index);

    String getString(int index);
}
