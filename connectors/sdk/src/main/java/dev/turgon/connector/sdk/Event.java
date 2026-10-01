package dev.turgon.connector.sdk;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A business event read from the system.
 *
 * @param id unique within the source and stable across re-reads
 * @param position orders events within the source; the worker's cursor stores it
 */
public record Event(String id, long position, String name, JsonNode payload) {}
