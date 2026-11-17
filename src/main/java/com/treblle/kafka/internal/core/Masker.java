package com.treblle.kafka.internal.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces sensitive values with asterisks before anything leaves the host application.
 *
 * <p>The keys listed in {@code maskedKeywords} (the SDK's defaults plus any configured ones) are
 * masked, matched case-insensitively.
 * Masking preserves structure and length so the shape of the data stays readable in Treblle: keys
 * survive, arrays keep their length, and {@code Bearer abc123} becomes {@code Bearer ******} so
 * users can still tell which auth scheme was used.
 *
 * <p>Free text the SDK cannot parse as key/value structure - exception messages - is scrubbed for
 * {@code keyword=value} and {@code keyword: value} pairs, which is how secrets usually leak into
 * error messages.
 */
public final class Masker {

    private static final Set<String> AUTH_SCHEMES = new HashSet<>(java.util.Arrays.asList(
            "bearer", "basic", "digest", "token", "apikey", "negotiate", "oauth", "hmac"));

    private final Set<String> keywords;
    private final Pattern textPattern;

    /** @param keywords keys to mask, expected to already be lowercased */
    public Masker(Set<String> keywords) {
        this.keywords = keywords == null ? java.util.Collections.emptySet() : keywords;
        this.textPattern = buildTextPattern(this.keywords);
    }

    /**
     * Matches {@code keyword}, an optional closing quote, {@code =} or {@code :}, an optional
     * opening quote and auth scheme - then captures the value up to the next delimiter.
     */
    private static Pattern buildTextPattern(Set<String> keywords) {
        if (keywords.isEmpty()) {
            return null;
        }
        StringBuilder alternatives = new StringBuilder();
        for (String keyword : keywords) {
            if (alternatives.length() > 0) {
                alternatives.append('|');
            }
            alternatives.append(Pattern.quote(keyword));
        }
        return Pattern.compile(
                "(?<![\\w-])((?:" + alternatives + ")[\"']?\\s*[=:]\\s*[\"']?"
                        + "(?:(?:bearer|basic|token|digest)\\s+)?)([^\\s\"',;&}\\]]+)",
                Pattern.CASE_INSENSITIVE);
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

    /** Masks {@code keyword=value} / {@code keyword: value} pairs inside free text. */
    public String maskText(String text) {
        if (textPattern == null || text == null || text.isEmpty()) {
            return text;
        }
        Matcher matcher = textPattern.matcher(text);
        if (!matcher.find()) {
            return text;
        }
        StringBuffer result = new StringBuffer(text.length());
        do {
            matcher.appendReplacement(result,
                    Matcher.quoteReplacement(matcher.group(1) + stars(matcher.group(2).length())));
        } while (matcher.find());
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Masks metadata entries whose key is a masked keyword. Values are scalars, so a match is
     * replaced outright.
     */
    public Map<String, Object> maskMetadata(Map<String, Object> metadata) {
        if (!isActive() || metadata == null || metadata.isEmpty()) {
            return metadata;
        }
        Map<String, Object> masked = new LinkedHashMap<>(metadata.size());
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            masked.put(entry.getKey(), matches(entry.getKey())
                    ? maskScalar(entry.getValue())
                    : entry.getValue());
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
