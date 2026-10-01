package com.sap.conn.jco.ext;

public interface DestinationDataEventListener {
    void deleted(String destinationName);

    void updated(String destinationName);
}
