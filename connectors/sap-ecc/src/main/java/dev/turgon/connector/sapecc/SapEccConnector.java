package dev.turgon.connector.sapecc;

import dev.turgon.connector.sdk.Capability;
import dev.turgon.connector.sdk.Connector;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.Endpoint;
import dev.turgon.connector.sdk.EndpointConfig;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * SAP ECC 6.0 through BAPIs over RFC (examples/connectors/sap-ecc.yaml):
 * create-sales-order (test run first), cancel-sales-order (its
 * compensation) and get-customer; and events from the IDocs SAP sends to
 * a registered server program (idoc-outbound).
 */
public final class SapEccConnector implements Connector {
    public static final String NAME = "sap-ecc";
    public static final String VERSION = "0.4.0";

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
        return EnumSet.of(Capability.SIMULATE, Capability.CONFIRM, Capability.READ, Capability.CHECK, Capability.STREAM);
    }

    /** The RFC providers on the class path. */
    static List<RfcProvider> providers() {
        List<RfcProvider> all = new ArrayList<>();
        ServiceLoader.load(RfcProvider.class).forEach(all::add);
        return all;
    }

    /** The configured provider, or the first available one (JCo first). */
    static RfcProvider provider(String name) throws ConnectorException.Invalid {
        List<RfcProvider> all = providers();
        if (!name.isEmpty()) {
            return all.stream().filter(p -> p.name().equals(name)).findFirst()
                    .orElseThrow(() -> new ConnectorException.Invalid("no RFC provider " + name + " in this connector"));
        }
        return all.stream().filter(RfcProvider::available).findFirst().orElse(all.stream()
                .filter(p -> p.name().equals("jco")).findFirst().orElseThrow());
    }

    @Override
    public Endpoint bind(EndpointConfig endpoint) throws Exception {
        int maxCalls = 0; // the worker's governor limits calls; JCo's pool is sized by the config
        EccConfig cfg = EccConfig.from(endpoint, maxCalls);
        RfcProvider p = provider(cfg.provider());
        if (!p.available()) {
            throw new ConnectorException.Invalid("RFC provider " + p.name() + " is not available: " + p.unavailableFix());
        }
        Rfc rfc = p.open(endpoint, cfg);
        return new EccEndpoint(endpoint.endpoint(), cfg, rfc, receiver -> p.serve(endpoint, cfg, rfc, receiver));
    }
}
