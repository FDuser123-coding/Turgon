package dev.turgon.connector.sapecc;

import java.util.List;
import java.util.Map;

/**
 * Remote function calls to an SAP system. Parameters and results use plain
 * values: a {@code String} for a scalar, a {@code Map<String, String>} for a
 * structure, a {@code List<Map<String, String>>} for a table. Results hold
 * the export, changing and table parameters.
 */
public interface Rfc extends AutoCloseable {

    /** Calls a function module. A communication failure throws; an ABAP exception throws {@link AbapError}. */
    Map<String, Object> call(String function, Map<String, Object> params) throws Exception;

    /**
     * Runs work on one connection, as one SAP session: a BAPI's changes are
     * kept only if BAPI_TRANSACTION_COMMIT runs in the same session.
     */
    <T> T session(Work<T> work) throws Exception;

    /** RFC_PING. */
    void ping() throws Exception;

    /** Whether the function module exists (and the user may see its interface). */
    boolean exists(String function) throws Exception;

    /** What the calls go to, for checks and logs: never credentials. */
    String describe();

    @Override
    default void close() throws Exception {}

    @FunctionalInterface
    interface Work<T> {
        T run(Rfc session) throws Exception;
    }

    /** An exception the function module raised (an ABAP exception). */
    final class AbapError extends Exception {
        private static final long serialVersionUID = 1L;
        private final String key;

        public AbapError(String key, String message) {
            super(message == null || message.isEmpty() ? key : key + ": " + message);
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, String>> table(Map<String, Object> result, String name) {
        Object v = result.get(name);
        if (v instanceof List<?> l) {
            return (List<Map<String, String>>) l;
        }
        if (v instanceof Map<?, ?> m) {
            return m.isEmpty() ? List.of() : List.of((Map<String, String>) m);
        }
        return List.of();
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> structure(Map<String, Object> result, String name) {
        Object v = result.get(name);
        return v instanceof Map<?, ?> m ? (Map<String, String>) m : Map.of();
    }
}
