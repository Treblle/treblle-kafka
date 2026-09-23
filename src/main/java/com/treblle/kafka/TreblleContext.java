package com.treblle.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Attaches custom metadata to the message currently being produced or consumed.
 *
 * <p>Metadata is thread-scoped. Set it on the thread that calls {@code send()}, or inside the
 * handler passed to {@code TreblleConsumerTracker.track(...)}:
 *
 * <pre>{@code
 * TreblleContext.put("tenantId", "acme");
 * producer.send(record);
 * }</pre>
 *
 * <p>The SDK takes a snapshot at capture time and clears the thread's context immediately, so
 * values never leak into the next message on a pooled thread.
 *
 * <p><strong>Metadata is never masked.</strong> Never put secrets, credentials or personal data
 * here.
 */
public final class TreblleContext {

    private static final ThreadLocal<Map<String, Object>> CONTEXT = new ThreadLocal<>();

    private TreblleContext() {
    }

    /** Attaches a string value to the current message. */
    public static void put(String key, String value) {
        store(key, value);
    }

    /** Attaches a numeric value to the current message. */
    public static void put(String key, Number value) {
        store(key, value);
    }

    /** Attaches a boolean value to the current message. */
    public static void put(String key, boolean value) {
        store(key, value);
    }

    /** Discards any metadata set on the current thread. */
    public static void clear() {
        CONTEXT.remove();
    }

    /**
     * Returns the current thread's metadata and clears it.
     *
     * <p>Internal SDK use. Never throws.
     */
    public static Map<String, Object> takeSnapshot() {
        Map<String, Object> current = CONTEXT.get();
        if (current == null || current.isEmpty()) {
            return Collections.emptyMap();
        }
        CONTEXT.remove();
        return current;
    }

    private static void store(String key, Object value) {
        try {
            if (key == null || key.trim().isEmpty() || value == null) {
                return;
            }
            Map<String, Object> current = CONTEXT.get();
            if (current == null) {
                current = new LinkedHashMap<>();
                CONTEXT.set(current);
            }
            current.put(key, value);
        } catch (RuntimeException ignored) {
            // Metadata is a convenience. It must never disrupt the host application.
        }
    }
}
