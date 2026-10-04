package com.sap.conn.jco;

public interface JCoTable extends JCoRecord {
    void appendRow();

    int getNumRows();

    void setRow(int row);
}
