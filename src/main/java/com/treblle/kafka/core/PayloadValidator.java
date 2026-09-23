package com.treblle.kafka.core;

import com.treblle.kafka.util.DebugLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The last gate before a payload is serialised and sent.
 *
 * <p>Treblle Ingress rejects payloads that deviate from the schema, and a rejected payload is a
 * silently lost request. Rather than drop anything, this validator <em>repairs</em>: it fills in
 * missing required keys with their schema defaults, coerces out-of-range values, and reports what
 * it fixed in debug mode. A capture with an unexpected shape still reaches Treblle.
 */
public final class PayloadValidator {

    private static final Pattern TIMESTAMP =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");

    private static final Set<String> HTTP_METHODS = new HashSet<>(Arrays.asList(
            "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"));

    private final DebugLogger logger;

    public PayloadValidator(DebugLogger logger) {
        this.logger = logger;
    }

    /** Repairs {@code payload} in place and returns it. Never throws. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> validate(Map<String, Object> payload) {
        try {
            requireString(payload, "sdk_token", "");
            requireString(payload, "api_key", "");
            requireString(payload, "sdk", PayloadBuilder.SDK_NAME);
            if (!(payload.get("version") instanceof Integer)) {
                repair("version", "not an integer");
                payload.put("version", PayloadBuilder.SDK_VERSION);
            }

            Object rawData = payload.get("data");
            if (!(rawData instanceof Map)) {
                repair("data", "missing");
                rawData = new LinkedHashMap<String, Object>();
                payload.put("data", rawData);
            }
            Map<String, Object> data = (Map<String, Object>) rawData;

            requireMap(data, "server");
            requireMap(data, "language");
            validateRequest(requireMap(data, "request"));
            validateResponse(requireMap(data, "response"));
            requireList(data, "errors");
            requireList(data, "queries");
        } catch (RuntimeException e) {
            logger.error("payload validation failed; sending as-is", e);
        }
        return payload;
    }

    private void validateRequest(Map<String, Object> request) {
        Object timestamp = request.get("timestamp");
        if (!(timestamp instanceof String) || !TIMESTAMP.matcher((String) timestamp).matches()) {
            repair("request.timestamp", "not in Y-m-d H:i:s format");
            request.put("timestamp", RuntimeInfo.formatTimestamp(System.currentTimeMillis()));
        }

        requireString(request, "ip", RuntimeInfo.UNKNOWN_IP);
        requireString(request, "url", "");
        requireString(request, "user_agent", "");

        Object method = request.get("method");
        if (!(method instanceof String) || !HTTP_METHODS.contains(method)) {
            repair("request.method", "not a supported method");
            request.put("method", "GET");
        }

        requireStringMap(request, "headers");
        requireStringMap(request, "query");
        if (!request.containsKey("body")) {
            repair("request.body", "missing");
            request.put("body", new LinkedHashMap<String, Object>());
        }
        if (!request.containsKey("route_path")) {
            repair("request.route_path", "missing");
            request.put("route_path", null);
        }
    }

    private void validateResponse(Map<String, Object> response) {
        requireStringMap(response, "headers");

        Object code = response.get("code");
        int status = code instanceof Number ? ((Number) code).intValue() : 200;
        if (status < 100 || status > 599) {
            repair("response.code", "outside 100-599");
            status = 500;
        }
        response.put("code", status);

        response.put("size", nonNegative(response.get("size"), "response.size"));
        response.put("load_time", nonNegative(response.get("load_time"), "response.load_time"));

        if (!response.containsKey("body")) {
            repair("response.body", "missing");
            response.put("body", new LinkedHashMap<String, Object>());
        }
    }

    private Number nonNegative(Object value, String field) {
        if (!(value instanceof Number)) {
            repair(field, "not a number");
            return 0;
        }
        Number number = (Number) value;
        if (number.doubleValue() < 0) {
            repair(field, "negative");
            return 0;
        }
        return number;
    }

    private void requireString(Map<String, Object> target, String key, String fallback) {
        if (!(target.get(key) instanceof String)) {
            repair(key, "missing or not a string");
            target.put(key, fallback);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> requireMap(Map<String, Object> target, String key) {
        Object value = target.get(key);
        if (!(value instanceof Map)) {
            repair(key, "missing or not an object");
            value = new LinkedHashMap<String, Object>();
            target.put(key, value);
        }
        return (Map<String, Object>) value;
    }

    private void requireList(Map<String, Object> target, String key) {
        if (!(target.get(key) instanceof List)) {
            repair(key, "missing or not an array");
            target.put(key, new ArrayList<>());
        }
    }

    /**
     * Headers and query parameters must be string-to-string. Anything else is stringified rather
     * than dropped, so the user still sees the value.
     */
    @SuppressWarnings("unchecked")
    private void requireStringMap(Map<String, Object> target, String key) {
        Object value = target.get(key);
        if (!(value instanceof Map)) {
            repair(key, "missing or not an object");
            target.put(key, new LinkedHashMap<String, Object>());
            return;
        }
        Map<Object, Object> source = (Map<Object, Object>) value;
        Map<String, String> normalised = new LinkedHashMap<>(source.size());
        for (Map.Entry<Object, Object> entry : source.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            normalised.put(entry.getKey().toString(),
                    entry.getValue() == null ? "" : entry.getValue().toString());
        }
        target.put(key, normalised);
    }

    private void repair(String field, String reason) {
        logger.warn("payload field '" + field + "' " + reason + "; repaired before sending");
    }
}
