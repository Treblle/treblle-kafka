package com.treblle.kafka.internal.core;

import com.treblle.kafka.ValueRenderer;
import com.treblle.kafka.internal.json.JsonParser;
import com.treblle.kafka.internal.json.JsonWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns a Kafka record value into something the Treblle payload schema accepts.
 *
 * <p>Treblle expects bodies to be valid JSON. Kafka carries arbitrary bytes, so this class decides
 * per value whether it can be represented faithfully, and substitutes a short explanatory JSON
 * object when it cannot. It never throws - an unparseable message must still produce a payload.
 *
 * <p>Every value it returns is a private copy made of plain JDK types, so later stages (masking,
 * serialisation) never touch an object the application might still be mutating.
 */
public final class BodyNormalizer {

    /** Treblle's hard body limit. Anything larger is replaced with a summary. */
    public static final int MAX_BODY_BYTES = 2 * 1024 * 1024;

    /** Size assumed for values whose size cannot be known in O(1), e.g. a POJO or a Map. */
    static final long NOMINAL_OBJECT_BYTES = 4 * 1024;

    private BodyNormalizer() {
    }

    /** Normalises a record value with no custom renderer. */
    public static Object normalize(Object value) {
        return normalize(value, null);
    }

    /**
     * Normalises a record value.
     *
     * <p>A null value is preserved as JSON null rather than replaced - in Kafka a null value is a
     * tombstone, which is meaningful information.
     *
     * @param renderer used for values of a type the SDK does not understand natively; may be null
     */
    public static Object normalize(Object value, ValueRenderer renderer) {
        try {
            return normalizeInternal(value, renderer);
        } catch (ConcurrentModificationException e) {
            return message("Message payload changed while Treblle was capturing it.",
                    value == null ? null : value.getClass().getName(), -1);
        } catch (RuntimeException | StackOverflowError e) {
            return message("Message payload could not be captured by Treblle.", null, -1);
        }
    }

    /**
     * Cheap, caller-thread check for values whose size is known in O(1). Returns a placeholder
     * body when {@code value} is over the limit, or null when it is fine (or its size is unknown).
     */
    public static Map<String, Object> oversizedPlaceholder(Object value) {
        long size = knownSize(value);
        return size > MAX_BODY_BYTES ? tooLarge(size) : null;
    }

    /** O(1) estimate of how much memory a captured value keeps alive. */
    public static long estimateBytes(Object value) {
        if (value == null) {
            return 0;
        }
        long size = knownSize(value);
        return size >= 0 ? size : NOMINAL_OBJECT_BYTES;
    }

    private static long knownSize(Object value) {
        if (value instanceof byte[]) {
            return ((byte[]) value).length;
        }
        if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).remaining();
        }
        if (value instanceof CharSequence) {
            // Lower bound on UTF-8 length: every char costs at least one byte.
            return ((CharSequence) value).length();
        }
        return -1;
    }

    private static Object normalizeInternal(Object value, ValueRenderer renderer) {
        if (value == null) {
            // Kafka tombstone. Valid JSON null, and worth showing as-is.
            return null;
        }
        if (value instanceof CharSequence) {
            return fromText(value.toString(), value.getClass().getName());
        }
        if (value instanceof byte[]) {
            return fromBytes((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            return fromBytes(toArray((ByteBuffer) value));
        }
        if (value instanceof Number || value instanceof Boolean) {
            // Scalars are legal JSON bodies and the schema allows them.
            return value;
        }
        if (value instanceof Map || value instanceof Collection) {
            return fromStructure(value);
        }
        if (renderer != null) {
            Object rendered;
            try {
                rendered = renderer.render(value);
            } catch (Exception e) {
                return message("Message payload could not be rendered by the configured "
                        + "valueRenderer.", value.getClass().getName(), -1);
            }
            if (rendered != null) {
                // Render once only: a renderer returning another POJO falls back to toString().
                return normalizeInternal(rendered, null);
            }
        }
        // A POJO or an Avro GenericRecord. Many of these render as JSON via toString(); if not,
        // we say so rather than guessing. Configure a ValueRenderer to do better.
        return fromText(String.valueOf(value), value.getClass().getName());
    }

    /**
     * Deep-copies an application-owned Map/Collection by serialising it and parsing it back. That
     * both enforces the size limit and detaches the result from the application's object.
     */
    private static Object fromStructure(Object value) {
        String json = JsonWriter.write(value);
        if (json.length() > MAX_BODY_BYTES) {
            return tooLarge(json.length());
        }
        int byteLength = json.getBytes(StandardCharsets.UTF_8).length;
        if (byteLength > MAX_BODY_BYTES) {
            return tooLarge(byteLength);
        }
        return JsonParser.parse(json);
    }

    private static Object fromText(String text, String typeName) {
        // UTF-8 uses at least one byte per char, so a string longer than the limit is over it.
        // Checking that first avoids materialising a byte[] for an obviously oversized payload.
        if (text.length() > MAX_BODY_BYTES) {
            return tooLarge(text.length());
        }
        int byteLength = text.getBytes(StandardCharsets.UTF_8).length;
        if (byteLength > MAX_BODY_BYTES) {
            return tooLarge(byteLength);
        }
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return new LinkedHashMap<String, Object>();
        }
        try {
            return JsonParser.parse(trimmed);
        } catch (RuntimeException e) {
            return message("Message payload is not valid JSON.", typeName, byteLength);
        }
    }

    private static Object fromBytes(byte[] bytes) {
        if (bytes.length > MAX_BODY_BYTES) {
            return tooLarge(bytes.length);
        }
        String decoded = new String(bytes, StandardCharsets.UTF_8);
        String trimmed = decoded.trim();
        if (trimmed.isEmpty()) {
            return new LinkedHashMap<String, Object>();
        }
        try {
            return JsonParser.parse(trimmed);
        } catch (RuntimeException e) {
            // Binary formats land here: Avro, Protobuf, raw bytes.
            return message("Message payload is not valid JSON.", "application/octet-stream",
                    bytes.length);
        }
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static Map<String, Object> tooLarge(long byteLength) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", "Message payload is too large to be captured by Treblle. "
                + "The limit is 2 MB.");
        body.put("size", humanReadable(byteLength));
        return body;
    }

    private static Map<String, Object> message(String text, String typeName, long byteLength) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", text);
        if (typeName != null) {
            body.put("type", typeName);
        }
        if (byteLength >= 0) {
            body.put("size", humanReadable(byteLength));
        }
        return body;
    }

    private static String humanReadable(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format(java.util.Locale.ROOT, "%.2f KB", bytes / 1024.0);
        }
        return String.format(java.util.Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }
}
