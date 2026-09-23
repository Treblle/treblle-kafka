package com.treblle.kafka.core;

import com.treblle.kafka.json.JsonParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns a Kafka record value into something the Treblle payload schema accepts.
 *
 * <p>Treblle expects bodies to be valid JSON. Kafka carries arbitrary bytes, so this class decides
 * per value whether it can be represented faithfully, and substitutes a short explanatory JSON
 * object when it cannot. It never throws - an unparseable message must still produce a payload.
 */
public final class BodyNormalizer {

    /** Treblle's hard body limit. Anything larger is replaced with a summary. */
    public static final int MAX_BODY_BYTES = 2 * 1024 * 1024;

    private BodyNormalizer() {
    }

    /**
     * Normalises a record value.
     *
     * <p>A null value is preserved as JSON null rather than replaced - in Kafka a null value is a
     * tombstone, which is meaningful information.
     */
    public static Object normalize(Object value) {
        try {
            return normalizeInternal(value);
        } catch (RuntimeException e) {
            return message("Message payload could not be captured by Treblle.", null, -1);
        }
    }

    private static Object normalizeInternal(Object value) {
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
            // Already a JSON-shaped structure (e.g. a JsonNode-free Map from a custom serialiser).
            return value;
        }
        // A POJO or an Avro GenericRecord. Many of these render as JSON via toString(); if not,
        // we say so rather than guessing. Schema-registry decoding is out of scope.
        return fromText(String.valueOf(value), value.getClass().getName());
    }

    private static Object fromText(String text, String typeName) {
        // UTF-8 uses at least one byte per char, so a string longer than the limit is over it.
        // Checking that first avoids materialising a byte[] for an obviously oversized payload.
        if (text.length() > MAX_BODY_BYTES) {
            return tooLarge(approximateByteLength(text));
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

    /**
     * Lower bound on UTF-8 length - every char costs at least one byte. Used to report a size for
     * an oversized payload without allocating a byte[] we would immediately discard.
     */
    private static int approximateByteLength(String text) {
        return text.length();
    }

    private static Map<String, Object> tooLarge(int byteLength) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("message", "Message payload is too large to be captured by Treblle. "
                + "The limit is 2 MB.");
        body.put("size", humanReadable(byteLength));
        return body;
    }

    private static Map<String, Object> message(String text, String typeName, int byteLength) {
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

    private static String humanReadable(int bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format(java.util.Locale.ROOT, "%.2f KB", bytes / 1024.0);
        }
        return String.format(java.util.Locale.ROOT, "%.2f MB", bytes / (1024.0 * 1024.0));
    }
}
