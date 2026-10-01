package dev.turgon.connector.sapecc.fake;

import dev.turgon.connector.sapecc.EccConfig;
import dev.turgon.connector.sapecc.Rfc;
import dev.turgon.connector.sapecc.RfcProvider;
import dev.turgon.connector.sdk.EndpointConfig;

/** RFC to an in-memory ECC ({@link FakeEcc}), one per destination host. */
public final class FakeRfcProvider implements RfcProvider {
    @Override
    public String name() {
        return "fake";
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public String unavailableFix() {
        return "";
    }

    @Override
    public Rfc open(EndpointConfig endpoint, EccConfig config) {
        String host = config.destination().getOrDefault("jco.client.ashost", "fake");
        return new FakeEcc.Connection(FakeEcc.system(host), config.destination().getOrDefault("jco.client.user", ""));
    }
}
