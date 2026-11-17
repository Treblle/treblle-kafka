package com.treblle.kafka;

/**
 * Turns an application object into something Treblle can display as a JSON body.
 *
 * <p>The SDK understands JSON text, {@code byte[]}/{@code ByteBuffer} containing JSON,
 * {@code Map}/{@code Collection} structures and scalars out of the box. Any other record value -
 * typically a POJO your producer serialises with Jackson, Gson or similar - is handed to this
 * renderer first. Plug in the serialiser you already use:
 *
 * <pre>{@code
 * ObjectMapper mapper = new ObjectMapper();
 * Treblle treblle = Treblle.builder()
 *         .valueRenderer(mapper::writeValueAsString)
 *         .build();
 * }</pre>
 *
 * <p>The renderer runs on the SDK's background worker thread, never on your producer or consumer
 * thread. It may return a JSON {@code String}, a {@code byte[]} of JSON, a {@code Map} or
 * {@code Collection}, or {@code null} to fall back to the default behaviour (the value's
 * {@code toString()}). Anything it throws is caught and the body is replaced with a short
 * explanation; it never reaches your application.
 */
@FunctionalInterface
public interface ValueRenderer {

    /**
     * Renders {@code value}, which is never null and never one of the types the SDK already
     * understands.
     */
    Object render(Object value) throws Exception;
}
