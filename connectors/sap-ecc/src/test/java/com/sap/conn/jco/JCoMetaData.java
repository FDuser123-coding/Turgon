package com.sap.conn.jco;

public interface JCoMetaData {
    boolean hasField(String name);

    int getFieldCount();

    String getName(int index);

    boolean isTable(int index);

    boolean isStructure(int index);
}
