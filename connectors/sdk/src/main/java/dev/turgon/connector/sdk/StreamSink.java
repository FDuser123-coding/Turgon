package dev.turgon.connector.sdk;

import java.util.List;

/** Where a pushed event goes: to the worker, over the open stream. */
public interface StreamSink {
    /**
     * Sends a batch and waits until the worker has stored it durably. Throws
     * when the stream is closed or the worker does not acknowledge in time:
     * the system must then keep the events and send them again.
     *
     * @param resume where to resume after this batch, if the system has positions; may be null
     */
    void deliver(List<Event> events, byte[] resume) throws Exception;

    /** Whether the worker still listens. */
    boolean open();
}
