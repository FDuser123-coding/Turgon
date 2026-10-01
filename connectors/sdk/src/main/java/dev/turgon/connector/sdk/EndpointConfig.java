package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Map;

/**
 * An endpoint's compiled configuration (the runtime spec's connector entry)
 * and its secrets, resolved by the worker from its secret manager.
 *
 * @param endpoint the endpoint's name in the spec, such as {@code sap-ecc}
 * @param connector the connector's name
 * @param version the connector version the spec was compiled for
 * @param config the connection's connector-specific configuration; an empty object when none
 * @param secret the value of the endpoint's secretRef
 * @param secrets the values of the secretRefs inside {@code config}, by reference
 */
public record EndpointConfig(String endpoint, String connector, String version, JsonNode config,
        String secret, Map<String, String> secrets) {

    public EndpointConfig {
        config = config == null || config.isNull() ? JsonNodeFactory.instance.objectNode() : config;
        secrets = Map.copyOf(secrets);
    }

    /** Never prints secrets. */
    @Override
    public String toString() {
        return "EndpointConfig[endpoint=" + endpoint + ", connector=" + connector + ", version=" + version + "]";
    }
}
