package com.treblle.kafka.json;

/**
 * Thrown when a document cannot be parsed as JSON.
 *
 * <p>This is always caught inside the SDK - it never escapes to the host application. It exists
 * so {@code BodyNormalizer} can tell "this was not JSON" apart from "this was JSON containing
 * null".
 */
public class JsonException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JsonException(String message) {
        super(message);
    }
}
