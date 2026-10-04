package dev.turgon.connector.sdk;

/** What a connector implements beyond commit; the protocol's capability names. */
public enum Capability {
    SIMULATE("simulate"),
    CONFIRM("confirm"),
    READ("read"),
    POLL("poll"),
    EXPORT("export"),
    CHECK("check"),
    STREAM("stream");

    private final String wire;

    Capability(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }
}
