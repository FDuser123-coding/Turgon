package dev.turgon.connector.sdk;

/** Failures the worker treats differently from transient ones. */
public abstract sealed class ConnectorException extends Exception {
    private static final long serialVersionUID = 1L;

    ConnectorException(String message) {
        super(message);
    }

    /** The payload or ID is not valid for the system: not retried. */
    public static final class Invalid extends ConnectorException {
        private static final long serialVersionUID = 1L;

        public Invalid(String message) {
            super(message);
        }
    }

    /** The system refused the write (a business rule): not retried; a saga compensates. */
    public static final class Rejected extends ConnectorException {
        private static final long serialVersionUID = 1L;

        public Rejected(String message) {
            super(message);
        }
    }

    /** No such record. */
    public static final class NotFound extends ConnectorException {
        private static final long serialVersionUID = 1L;

        public NotFound(String message) {
            super(message);
        }
    }

    /** The connector does not implement this. */
    public static final class Unsupported extends ConnectorException {
        private static final long serialVersionUID = 1L;

        public Unsupported(String what) {
            super("the connector does not support " + what);
        }
    }
}
