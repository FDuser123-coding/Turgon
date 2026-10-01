package dev.turgon.connector.sdk;

/** One connectivity or configuration check, with a plain-language fix when it fails. */
public record CheckResult(String name, boolean ok, String detail, String fix) {
    public static CheckResult pass(String name, String detail) {
        return new CheckResult(name, true, detail, "");
    }

    public static CheckResult fail(String name, String detail, String fix) {
        return new CheckResult(name, false, detail, fix);
    }
}
