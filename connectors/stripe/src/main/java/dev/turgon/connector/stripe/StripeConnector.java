package dev.turgon.connector.stripe;

import dev.turgon.connector.sdk.Capability;
import dev.turgon.connector.sdk.Connector;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.util.EnumSet;
import java.util.Set;

/**
 * Stripe billing through the REST API (examples/connectors/stripe.yaml):
 * paid invoices as Invoice.Paid events, create-invoice with its
 * compensation void-invoice, link-invoice with unlink-invoice, and reads
 * of invoices and customers.
 */
public final class StripeConnector implements Connector {
    public static final String NAME = "stripe";
    public static final String VERSION = "1.5.0";

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
        return EnumSet.of(Capability.SIMULATE, Capability.CONFIRM, Capability.READ, Capability.POLL, Capability.CHECK);
    }

    @Override
    public Endpoint bind(EndpointConfig endpoint) throws Exception {
        StripeConfig cfg = StripeConfig.from(endpoint);
        return new StripeEndpoint(endpoint.endpoint(), cfg, new StripeClient(cfg, endpoint.secret()));
    }
}
