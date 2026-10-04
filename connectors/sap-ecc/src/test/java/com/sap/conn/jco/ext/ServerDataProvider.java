package com.sap.conn.jco.ext;

import java.util.Properties;

public interface ServerDataProvider {
    Properties getServerProperties(String serverName);

    boolean supportsEvents();

    void setServerDataEventListener(ServerDataEventListener listener);
}
