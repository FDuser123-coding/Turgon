package com.sap.conn.jco.ext;

public interface ServerDataEventListener {
    void deleted(String serverName);

    void updated(String serverName);
}
