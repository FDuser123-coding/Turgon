package com.sap.conn.jco.ext;

import java.util.ArrayList;
import java.util.List;

public final class Environment {
    public static volatile DestinationDataProvider provider;
    public static volatile ServerDataProvider serverProvider;

    public static synchronized void registerServerDataProvider(ServerDataProvider p) {
        if (serverProvider != null) {
            throw new IllegalStateException("ServerDataProvider already registered");
        }
        serverProvider = p;
    }
    /** Destinations JCo was told were deleted. */
    public static final List<String> DELETED = new ArrayList<>();

    private Environment() {}

    public static synchronized void registerDestinationDataProvider(DestinationDataProvider p) {
        if (provider != null) {
            throw new IllegalStateException("DestinationDataProvider already registered");
        }
        provider = p;
        p.setDestinationDataEventListener(new DestinationDataEventListener() {
            @Override
            public void deleted(String name) {
                DELETED.add(name);
            }

            @Override
            public void updated(String name) {}
        });
    }
}
