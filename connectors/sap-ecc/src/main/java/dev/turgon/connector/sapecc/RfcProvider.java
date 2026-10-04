package dev.turgon.connector.sapecc;

import dev.turgon.connector.sdk.EndpointConfig;

/**
 * Opens RFC connections. Providers are found with {@link java.util.ServiceLoader}:
 * {@code jco} (SAP JCo, when its library is installed) ships with the connector;
 * a test fake can be added to the class path.
 */
public interface RfcProvider {
    String name();

    /** Whether the provider can run here (JCo: its library is on the class path). */
    boolean available();

    /** What to do when it is not available. */
    String unavailableFix();

    Rfc open(EndpointConfig endpoint, EccConfig config) throws Exception;

    /**
     * Registers a server program at the SAP gateway (config.idoc()) and
     * hands the IDocs SAP sends it to receiver, until closed. rfc is the
     * client connection, whose repository describes the functions.
     */
    default AutoCloseable serve(EndpointConfig endpoint, EccConfig config, Rfc rfc, IdocReceiver receiver) throws Exception {
        throw new UnsupportedOperationException("RFC provider " + name() + " cannot receive IDocs");
    }
}
