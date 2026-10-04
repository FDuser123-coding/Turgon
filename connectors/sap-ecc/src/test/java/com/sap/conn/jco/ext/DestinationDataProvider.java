package com.sap.conn.jco.ext;

import java.util.Properties;

public interface DestinationDataProvider {
    Properties getDestinationProperties(String destinationName);

    boolean supportsEvents();

    void setDestinationDataEventListener(DestinationDataEventListener listener);
}
