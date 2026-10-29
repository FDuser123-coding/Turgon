package dev.turgon.connector.shopify;

import dev.turgon.connector.sdk.Capability;
import dev.turgon.connector.sdk.Connector;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.util.EnumSet;
import java.util.Set;

/**
 * Shopify through the GraphQL Admin API (examples/connectors/shopify.yaml):
 * new orders as Order.Created events, get-order, get-customer, and
 * update-order with its compensation restore-order.
 */
public final class ShopifyConnector implements Connector {
    public static final String NAME = "shopify";
    public static final String VERSION = "2.1.0";

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
        return EnumSet.of(Capability.SIMULATE, Capability.CONFIRM, Capability.READ, Capability.POLL, Capability.CHECK, Capability.DISCOVER,
                Capability.WEBHOOK);
    }

    @Override
    public Endpoint bind(EndpointConfig endpoint) throws Exception {
        ShopifyConfig cfg = ShopifyConfig.from(endpoint);
        return new ShopifyEndpoint(endpoint.endpoint(), cfg, new ShopifyClient(cfg, endpoint.secret()));
    }
}
