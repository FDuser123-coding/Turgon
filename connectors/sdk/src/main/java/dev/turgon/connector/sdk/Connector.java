package dev.turgon.connector.sdk;

import java.util.Set;

/**
 * A connector type, such as sap-ecc: it binds endpoints to their systems.
 * Implementations are found with {@link java.util.ServiceLoader}, so one
 * sidecar image can carry several.
 */
public interface Connector {
    /** The connector manifest's name, such as {@code sap-ecc}. */
    String name();

    /** The connector's version, semver; the worker checks the spec was compiled for it. */
    String version();

    /** What endpoints implement beyond {@link Endpoint#commit}. */
    Set<Capability> capabilities();

    /**
     * Binds an endpoint: its compiled configuration and secrets. Called again
     * with a new instance when a secret rotates; the old one is closed once
     * the worker is done with it.
     */
    Endpoint bind(EndpointConfig config) throws Exception;
}
