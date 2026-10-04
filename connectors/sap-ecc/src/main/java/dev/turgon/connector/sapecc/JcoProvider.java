package dev.turgon.connector.sapecc;

import dev.turgon.connector.sdk.EndpointConfig;

/** SAP JCo, when sapjco3.jar (and its native library) is on the class path. */
public final class JcoProvider implements RfcProvider {
    @Override
    public String name() {
        return "jco";
    }

    @Override
    public boolean available() {
        try {
            Class.forName("com.sap.conn.jco.JCoDestinationManager", false, JcoProvider.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    @Override
    public String unavailableFix() {
        return "SAP JCo is licensed by SAP and not shipped: download SAP Java Connector 3.1 for Linux x86_64 from the SAP Support "
                + "Portal and mount sapjco3.jar and libsapjco3.so in /opt/turgon/lib (in the Helm chart, a volume in connectors.sidecars[].volumes).";
    }

    @Override
    public Rfc open(EndpointConfig endpoint, EccConfig config) throws Exception {
        return JcoRfc.open(endpoint.endpoint(), config);
    }

    @Override
    public AutoCloseable serve(EndpointConfig endpoint, EccConfig config, Rfc rfc, IdocReceiver receiver) throws Exception {
        return JcoIdocServer.start(endpoint.endpoint(), config, (JcoRfc) rfc, receiver);
    }
}
