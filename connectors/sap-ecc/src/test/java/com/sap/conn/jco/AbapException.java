package com.sap.conn.jco;

public class AbapException extends JCoException {
    private static final long serialVersionUID = 1L;
    private final String key;

    public AbapException(String key, String message) {
        super(message);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
