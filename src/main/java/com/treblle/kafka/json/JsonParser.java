package com.treblle.kafka.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small, dependency-free recursive-descent JSON parser.
 *
 * <p>The SDK ships with zero runtime dependencies, so it cannot lean on Jackson or Gson - pulling
 * either into a host application is a real dependency-conflict risk. This parser covers RFC 8259
 * and nothing more.
 *
 * <p>Values are materialised into plain JDK types so the rest of the SDK never needs a node
 * abstraction:
 *
 * <ul>
 *   <li>object -&gt; {@link LinkedHashMap} (insertion ordered, so output is stable)
 *   <li>array -&gt; {@link ArrayList}
 *   <li>string -&gt; {@link String}
 *   <li>number -&gt; {@link Long} when integral and in range, otherwise {@link Double}
 *   <li>true/false -&gt; {@link Boolean}
 *   <li>null -&gt; {@code null}
 * </ul>
 *
 * <p>Nesting is capped at {@link #MAX_DEPTH} so a hostile or malformed message cannot blow the
 * stack of a Kafka I/O thread.
 */
public final class JsonParser {

    /** Maximum nesting depth. Deeper documents are rejected rather than parsed. */
    public static final int MAX_DEPTH = 64;

    private final String source;
    private int pos;

    private JsonParser(String source) {
        this.source = source;
        this.pos = 0;
    }

    /**
     * Parses {@code json} into plain JDK types.
     *
     * @throws JsonException if the input is not a single well-formed JSON value
     */
    public static Object parse(String json) {
        if (json == null) {
            throw new JsonException("input is null");
        }
        JsonParser parser = new JsonParser(json);
        parser.skipWhitespace();
        Object value = parser.readValue(0);
        parser.skipWhitespace();
        if (parser.pos != json.length()) {
            throw new JsonException("trailing content at position " + parser.pos);
        }
        return value;
    }

    /** Returns true when {@code json} parses cleanly. Never throws. */
    public static boolean isValid(String json) {
        try {
            parse(json);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private Object readValue(int depth) {
        if (depth > MAX_DEPTH) {
            throw new JsonException("nesting deeper than " + MAX_DEPTH + " levels");
        }
        if (pos >= source.length()) {
            throw new JsonException("unexpected end of input");
        }
        char c = source.charAt(pos);
        switch (c) {
            case '{':
                return readObject(depth);
            case '[':
                return readArray(depth);
            case '"':
                return readString();
            case 't':
                expect("true");
                return Boolean.TRUE;
            case 'f':
                expect("false");
                return Boolean.FALSE;
            case 'n':
                expect("null");
                return null;
            default:
                return readNumber();
        }
    }

    private Map<String, Object> readObject(int depth) {
        Map<String, Object> result = new LinkedHashMap<>();
        pos++; // consume '{'
        skipWhitespace();
        if (peek() == '}') {
            pos++;
            return result;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw new JsonException("expected object key at position " + pos);
            }
            String key = readString();
            skipWhitespace();
            if (peek() != ':') {
                throw new JsonException("expected ':' at position " + pos);
            }
            pos++;
            skipWhitespace();
            // Duplicate keys: last one wins, matching the rest of the SDK's header/query handling.
            result.put(key, readValue(depth + 1));
            skipWhitespace();
            char next = peek();
            if (next == ',') {
                pos++;
                continue;
            }
            if (next == '}') {
                pos++;
                return result;
            }
            throw new JsonException("expected ',' or '}' at position " + pos);
        }
    }

    private List<Object> readArray(int depth) {
        List<Object> result = new ArrayList<>();
        pos++; // consume '['
        skipWhitespace();
        if (peek() == ']') {
            pos++;
            return result;
        }
        while (true) {
            skipWhitespace();
            result.add(readValue(depth + 1));
            skipWhitespace();
            char next = peek();
            if (next == ',') {
                pos++;
                continue;
            }
            if (next == ']') {
                pos++;
                return result;
            }
            throw new JsonException("expected ',' or ']' at position " + pos);
        }
    }

    private String readString() {
        pos++; // consume opening quote
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= source.length()) {
                throw new JsonException("unterminated string");
            }
            char c = source.charAt(pos++);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\') {
                if (c < 0x20) {
                    throw new JsonException("unescaped control character in string");
                }
                sb.append(c);
                continue;
            }
            if (pos >= source.length()) {
                throw new JsonException("unterminated escape sequence");
            }
            char escape = source.charAt(pos++);
            switch (escape) {
                case '"':
                    sb.append('"');
                    break;
                case '\\':
                    sb.append('\\');
                    break;
                case '/':
                    sb.append('/');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'f':
                    sb.append('\f');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'u':
                    if (pos + 4 > source.length()) {
                        throw new JsonException("truncated \\u escape");
                    }
                    String hex = source.substring(pos, pos + 4);
                    try {
                        sb.append((char) Integer.parseInt(hex, 16));
                    } catch (NumberFormatException e) {
                        throw new JsonException("invalid \\u escape: " + hex);
                    }
                    pos += 4;
                    break;
                default:
                    throw new JsonException("invalid escape character: \\" + escape);
            }
        }
    }

    private Object readNumber() {
        int start = pos;
        if (peek() == '-') {
            pos++;
        }
        while (pos < source.length() && isNumberChar(source.charAt(pos))) {
            pos++;
        }
        String literal = source.substring(start, pos);
        if (literal.isEmpty() || literal.equals("-")) {
            throw new JsonException("invalid value at position " + start);
        }
        boolean fractional = literal.indexOf('.') >= 0
                || literal.indexOf('e') >= 0
                || literal.indexOf('E') >= 0;
        try {
            if (!fractional) {
                return Long.valueOf(literal);
            }
        } catch (NumberFormatException ignored) {
            // Integral but wider than a long - fall through to double.
        }
        try {
            double parsed = Double.parseDouble(literal);
            if (Double.isNaN(parsed) || Double.isInfinite(parsed)) {
                throw new JsonException("number out of range: " + literal);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new JsonException("invalid number: " + literal);
        }
    }

    private static boolean isNumberChar(char c) {
        return (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-';
    }

    private void expect(String literal) {
        if (!source.startsWith(literal, pos)) {
            throw new JsonException("expected '" + literal + "' at position " + pos);
        }
        pos += literal.length();
    }

    private char peek() {
        if (pos >= source.length()) {
            throw new JsonException("unexpected end of input");
        }
        return source.charAt(pos);
    }

    private void skipWhitespace() {
        while (pos < source.length()) {
            char c = source.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                pos++;
            } else {
                return;
            }
        }
    }
}
