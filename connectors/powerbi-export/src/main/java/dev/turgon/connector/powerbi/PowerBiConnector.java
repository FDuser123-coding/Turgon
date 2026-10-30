package dev.turgon.connector.powerbi;

import dev.turgon.connector.sdk.Capability;
import dev.turgon.connector.sdk.Connector;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Set;

/**
 * Power BI push datasets through the REST API, as a Microsoft Entra service
 * principal (examples/connectors/powerbi-export.yaml): push-rows, its
 * compensation reverse-rows, and delete-rows.
 */
public final class PowerBiConnector implements Connector {
    public static final String NAME = "powerbi-export";
    public static final String VERSION = "1.2.0";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.of(Capability.SIMULATE, Capability.CHECK, Capability.DISCOVER);
    }

    @Override
    public Endpoint bind(EndpointConfig endpoint) throws Exception {
        PowerBiConfig cfg = PowerBiConfig.from(endpoint);
        return new PowerBiEndpoint(endpoint.endpoint(), cfg, new PowerBiClient(cfg, endpoint.secret(), Clock.systemUTC()));
    }
}
