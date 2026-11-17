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
 * <p>Lifecycle rules:
 *
 * <ul>
 *   <li>Outside a consume handler, every {@code send()} through a wrapped producer takes the
 *       metadata and clears it - whether or not that topic is tracked - so values never leak into
 *       the next message on a pooled thread.
 *   <li>Inside a consume handler, metadata belongs to the consumed message. It is cleared when the
 *       handler returns. A {@code send()} made from inside the handler (consume-transform-produce)
 *       is tagged with a <em>copy</em> of it, so both the consume and the produce carry it.
 *   <li>Metadata set before {@code track(...)} is not attached to the consumed message; it stays
 *       for the next {@code send()} outside the handler.
 * </ul>
 *
 * <p>Metadata whose key is a masked keyword is masked like any other value. Even so, do not put
 * secrets or personal data here.
 */
public final class TreblleContext {

    /** One level of metadata. Handlers open a nested frame so they never see or clear outer values. */
    private static final class Frame {
        final Frame parent;
        final boolean handlerScope;
        Map<String, Object> values;

        Frame(Frame parent, boolean handlerScope) {
            this.parent = parent;
            this.handlerScope = handlerScope;
        }
    }

    private static final ThreadLocal<Frame> CURRENT = new ThreadLocal<>();

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

    /** Discards any metadata set for the current message. */
    public static void clear() {
        Frame frame = CURRENT.get();
        if (frame == null) {
            return;
        }
        if (frame.handlerScope) {
            frame.values = null;
        } else {
            CURRENT.remove();
        }
    }

    // --- Internal API used by the collectors. -------------------------------------------------

    /**
     * Returns the metadata for a message being produced. Outside a handler the metadata is taken
     * and cleared; inside a handler a copy is returned and the handler's metadata is left intact.
     */
    static Map<String, Object> forSend() {
        Frame frame = CURRENT.get();
        if (frame == null) {
            return Collections.emptyMap();
        }
        if (frame.handlerScope) {
            return frame.values == null || frame.values.isEmpty()
                    ? Collections.<String, Object>emptyMap()
                    : new LinkedHashMap<>(frame.values);
        }
        CURRENT.remove();
        return frame.values == null ? Collections.<String, Object>emptyMap() : frame.values;
    }

    /** Opens a handler scope. Must be paired with {@link #closeHandlerScope()} in a finally. */
    static void openHandlerScope() {
        CURRENT.set(new Frame(CURRENT.get(), true));
    }

    /** Closes the innermost handler scope and returns the metadata set inside it. */
    static Map<String, Object> closeHandlerScope() {
        Frame frame = CURRENT.get();
        if (frame == null || !frame.handlerScope) {
            // Defensive: the user cleared or replaced the scope. Nothing sensible to return.
            return Collections.emptyMap();
        }
        if (frame.parent == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(frame.parent);
        }
        return frame.values == null ? Collections.<String, Object>emptyMap() : frame.values;
    }

    private static void store(String key, Object value) {
        try {
            if (key == null || key.trim().isEmpty() || value == null) {
                return;
            }
            Frame frame = CURRENT.get();
            if (frame == null) {
                frame = new Frame(null, false);
                CURRENT.set(frame);
            }
            if (frame.values == null) {
                frame.values = new LinkedHashMap<>();
            }
            frame.values.put(key, value);
        } catch (RuntimeException ignored) {
            // Metadata is a convenience. It must never disrupt the host application.
        }
    }
}
