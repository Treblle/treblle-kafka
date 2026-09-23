package com.treblle.kafka.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Replaces sensitive values with asterisks before anything leaves the host application.
 *
 * <p>Only the keys listed in {@code maskedKeywords} are masked, matched case-insensitively.
 * Masking preserves structure and length so the shape of the data stays readable in Treblle: keys
 * survive, arrays keep their length, and {@code Bearer abc123} becomes {@code Bearer ******} so
 * users can still tell which auth scheme was used.
 *
 * <p>An empty keyword set disables masking entirely, which the SDK warns about in debug mode.
 */
public final class Masker {

    private static final Set<String> AUTH_SCHEMES = new HashSet<>(java.util.Arrays.asList(
            "bearer", "basic", "digest", "token", "apikey", "negotiate", "oauth", "hmac"));

    private final Set<String> keywords;

    /** @param keywords keys to mask, expected to already be lowercased */
    public Masker(Set<String> keywords) {
        this.keywords = keywords == null ? java.util.Collections.emptySet() : keywords;
    }

    public boolean isActive() {
        return !keywords.isEmpty();
    }

    /**
     * Masks a parsed body. Objects are walked recursively; when a key matches, its entire value is
     * masked with keys and array structure preserved.
     */
    public Object maskBody(Object body) {
        if (!isActive()) {
            return body;
        }
        return walk(body, false, 0);
    }

    /**
     * Masks a header map. Matching values keep their auth scheme, so {@code authorization:
     * Bearer abc} becomes {@code Bearer ***}.
     */
    public Map<String, String> maskHeaders(Map<String, String> headers) {
        if (!isActive() || headers == null || headers.isEmpty()) {
            return headers;
        }
        Map<String, String> masked = new LinkedHashMap<>(headers.size());
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (matches(key) && value != null && !value.isEmpty()) {
                masked.put(key, maskPreservingAuthScheme(value));
            } else {
                masked.put(key, value);
            }
        }
        return masked;
    }

    private Object walk(Object node, boolean forceMask, int depth) {
        if (node == null || depth > 64) {
            return node;
        }
        if (node instanceof Map) {
            Map<?, ?> source = (Map<?, ?>) node;
            Map<String, Object> result = new LinkedHashMap<>(source.size());
            for (Map.Entry<?, ?> entry : source.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                String key = entry.getKey().toString();
                // Keys are always preserved; only the value is masked.
                result.put(key, walk(entry.getValue(), forceMask || matches(key), depth + 1));
            }
            return result;
        }
        if (node instanceof Collection) {
            Collection<?> source = (Collection<?>) node;
            List<Object> result = new ArrayList<>(source.size());
            for (Object item : source) {
                // Array items are masked individually; the array structure is preserved.
                result.add(walk(item, forceMask, depth + 1));
            }
            return result;
        }
        return forceMask ? maskScalar(node) : node;
    }

    private boolean matches(String key) {
        return key != null && keywords.contains(key.toLowerCase(Locale.ROOT));
    }

    private static Object maskScalar(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        if (text.isEmpty()) {
            return value;
        }
        return stars(text.length());
    }

    private static String maskPreservingAuthScheme(String value) {
        int space = value.indexOf(' ');
        if (space > 0 && space < value.length() - 1) {
            String scheme = value.substring(0, space);
            if (AUTH_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT))) {
                return scheme + " " + stars(value.length() - space - 1);
            }
        }
        return stars(value.length());
    }

    private static String stars(int length) {
        char[] chars = new char[length];
        java.util.Arrays.fill(chars, '*');
        return new String(chars);
    }
}
