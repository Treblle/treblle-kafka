package com.treblle.kafka.json;

import java.util.Collection;
import java.util.Map;

/**
 * A small, dependency-free JSON serialiser.
 *
 * <p>Accepts the same plain JDK types {@link JsonParser} produces. Anything it does not recognise
 * is written as its {@code toString()} in quotes rather than throwing - the SDK must never fail a
 * send because of an unexpected value type.
 */
public final class JsonWriter {

    private JsonWriter() {
    }

    /** Serialises {@code value} to a compact JSON document. */
    public static String write(Object value) {
        StringBuilder sb = new StringBuilder(256);
        writeValue(sb, value, 0);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object value, int depth) {
        if (depth > JsonParser.MAX_DEPTH) {
            // Defensive: a cyclic structure would otherwise recurse forever.
            sb.append("null");
            return;
        }
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            writeString(sb, (String) value);
        } else if (value instanceof Boolean) {
            sb.append(value.toString());
        } else if (value instanceof Number) {
            writeNumber(sb, (Number) value);
        } else if (value instanceof Map) {
            writeObject(sb, (Map<?, ?>) value, depth);
        } else if (value instanceof Collection) {
            writeArray(sb, (Collection<?>) value, depth);
        } else if (value instanceof Object[]) {
            writeArray(sb, java.util.Arrays.asList((Object[]) value), depth);
        } else {
            writeString(sb, value.toString());
        }
    }

    private static void writeObject(StringBuilder sb, Map<?, ?> map, int depth) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeString(sb, entry.getKey().toString());
            sb.append(':');
            writeValue(sb, entry.getValue(), depth + 1);
        }
        sb.append('}');
    }

    private static void writeArray(StringBuilder sb, Collection<?> items, int depth) {
        sb.append('[');
        boolean first = true;
        for (Object item : items) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            writeValue(sb, item, depth + 1);
        }
        sb.append(']');
    }

    private static void writeNumber(StringBuilder sb, Number number) {
        if (number instanceof Double || number instanceof Float) {
            double d = number.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                // Not representable in JSON. Emit null rather than an invalid document.
                sb.append("null");
                return;
            }
        }
        sb.append(number.toString());
    }

    private static void writeString(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
