package com.treblle.kafka.core;

import com.treblle.kafka.TreblleConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a {@link Capture} into a Treblle Ingress payload.
 *
 * <p>This is where Kafka's event model is mapped onto Treblle's HTTP-shaped payload schema. No
 * schema field is added, removed or retyped - the translation happens entirely in the values:
 *
 * <table>
 *   <caption>Kafka to Treblle</caption>
 *   <tr><th>Treblle field</th><th>Produce</th><th>Consume</th></tr>
 *   <tr><td>{@code request.method}</td><td>POST</td><td>GET</td></tr>
 *   <tr><td>{@code request.route_path}</td><td colspan="2">topic name (the AsyncAPI channel)</td></tr>
 *   <tr><td>{@code request.url}</td><td colspan="2">{@code kafka://broker/topic}</td></tr>
 *   <tr><td>{@code request.body}</td><td colspan="2">the record value</td></tr>
 *   <tr><td>{@code response.code}</td><td>broker ack</td><td>handler outcome</td></tr>
 *   <tr><td>{@code response.load_time}</td><td>send to ack</td><td>handler duration</td></tr>
 *   <tr><td>{@code response.body}</td><td>ack receipt</td><td>handler return value</td></tr>
 * </table>
 *
 * <p>Producing to {@code orders.created} therefore renders in Treblle as {@code POST
 * /orders.created} and consuming it as {@code GET /orders.created} - the producer view and the
 * consumer view of the same channel, as two distinct endpoints. {@code metadata["kafka.operation"]}
 * carries the true produce/consume semantic for when Treblle grows real AsyncAPI verbs.
 *
 * <p>Each topic is auto-discovered as its own Treblle API: {@code internal_name} is the topic name
 * and {@code internal_id} folds in the cluster identity ({@code <cluster>:<topic>}), so a single SDK
 * token fronting many topics - or many clusters - fans out into one API per topic rather than one
 * API per cluster.
 *
 * <p>This class is framework-agnostic: it imports nothing from Kafka and can be exercised in
 * isolation.
 */
public final class PayloadBuilder {

    /** Kebab-case SDK identifier sent to Ingress. */
    public static final String SDK_NAME = "kafka";

    /** Treblle encodes versions as integers: 10 = 1.0. */
    public static final int SDK_VERSION = 10;

    private static final int MAX_METADATA_ENTRIES = 20;
    private static final int MAX_METADATA_KEY_LENGTH = 64;
    private static final int MAX_METADATA_VALUE_LENGTH = 128;

    private final TreblleConfig config;
    private final Masker masker;
    private final String kafkaClientVersion;

    public PayloadBuilder(TreblleConfig config, Masker masker, String kafkaClientVersion) {
        this.config = config;
        this.masker = masker;
        this.kafkaClientVersion = kafkaClientVersion == null ? "unknown" : kafkaClientVersion;
    }

    /** Builds a complete, schema-valid payload. */
    public Map<String, Object> build(Capture capture) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sdk_token", config.getSdkToken());
        payload.put("api_key", config.getApiKey());
        payload.put("sdk", SDK_NAME);
        payload.put("version", SDK_VERSION);

        // Each Kafka topic is its own logical async API - the same situation internal_id /
        // internal_name were built for (one credential pair fronting many auto-discovered APIs).
        // The topic name alone is internal_name; internal_id folds in the cluster identity too, so
        // two clusters that share credentials and a topic name never collapse into one API.
        if (isNotEmpty(capture.topic)) {
            payload.put("internal_id", buildInternalId(capture));
            payload.put("internal_name", capture.topic);
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("server", buildServer());
        data.put("language", buildLanguage());
        data.put("request", buildRequest(capture));
        data.put("response", buildResponse(capture));
        data.put("errors", buildErrors(capture));
        // Kafka has no SQL layer. The field is required, so it is always an empty array.
        data.put("queries", new ArrayList<>());
        data.put("metadata", buildMetadata(capture));
        payload.put("data", data);

        return payload;
    }

    private Map<String, Object> buildServer() {
        Map<String, Object> os = new LinkedHashMap<>();
        os.put("name", RuntimeInfo.osName());
        os.put("release", RuntimeInfo.osVersion());
        os.put("architecture", RuntimeInfo.osArchitecture());

        Map<String, Object> server = new LinkedHashMap<>();
        server.put("ip", RuntimeInfo.serverIp());
        server.put("timezone", RuntimeInfo.timezone());
        server.put("software", "apache-kafka-clients/" + kafkaClientVersion);
        server.put("protocol", "KAFKA");
        server.put("os", os);
        return server;
    }

    private Map<String, Object> buildLanguage() {
        Map<String, Object> language = new LinkedHashMap<>();
        language.put("name", "java");
        language.put("version", RuntimeInfo.javaVersion());
        return language;
    }

    private Map<String, Object> buildRequest(Capture capture) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("timestamp", RuntimeInfo.formatTimestamp(capture.recordTimestampMillis));
        request.put("ip", RuntimeInfo.serverIp());
        request.put("url", buildUrl(capture));
        request.put("user_agent", buildUserAgent(capture));
        request.put("method", capture.isProduce() ? "POST" : "GET");
        request.put("headers", masker.maskHeaders(nonNull(capture.headers)));
        request.put("body", masker.maskBody(BodyNormalizer.normalize(capture.value)));
        request.put("route_path", capture.topic);
        // Kafka has no query string. The field is required, so it is always an empty object.
        request.put("query", new LinkedHashMap<String, Object>());
        return request;
    }

    private Map<String, Object> buildResponse(Capture capture) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("headers", masker.maskHeaders(buildResponseHeaders(capture)));
        response.put("code", ErrorMapper.statusFor(capture.failure));
        response.put("size", size(capture));
        response.put("load_time", round(capture.durationMillis));
        response.put("body", masker.maskBody(buildResponseBody(capture)));
        return response;
    }

    private Map<String, String> buildResponseHeaders(Capture capture) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("kafka-topic", capture.topic);
        if (capture.partition >= 0) {
            headers.put("kafka-partition", String.valueOf(capture.partition));
        }
        if (capture.offset >= 0) {
            headers.put("kafka-offset", String.valueOf(capture.offset));
        }
        if (capture.isProduce()) {
            if (capture.ackTimestampMillis > 0) {
                headers.put("kafka-timestamp",
                        RuntimeInfo.formatTimestamp(capture.ackTimestampMillis));
            }
        } else {
            if (isNotEmpty(capture.groupId)) {
                headers.put("kafka-group-id", capture.groupId);
            }
            if (capture.consumerLagMillis >= 0) {
                headers.put("kafka-lag-ms", String.valueOf(capture.consumerLagMillis));
            }
        }
        return headers;
    }

    private Object buildResponseBody(Capture capture) {
        if (capture.failure != null) {
            Throwable root = ErrorMapper.unwrap(capture.failure);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("message", root.getMessage() == null
                    ? root.getClass().getName()
                    : root.getMessage());
            return body;
        }
        if (capture.isProduce()) {
            // The broker's acknowledgement is the closest thing Kafka has to a response body.
            Map<String, Object> receipt = new LinkedHashMap<>();
            receipt.put("topic", capture.topic);
            receipt.put("partition", capture.partition);
            receipt.put("offset", capture.offset);
            if (capture.ackTimestampMillis > 0) {
                receipt.put("timestamp", capture.ackTimestampMillis);
            }
            return receipt;
        }
        if (capture.hasHandlerResult) {
            return BodyNormalizer.normalize(capture.handlerResult);
        }
        return new LinkedHashMap<String, Object>();
    }

    private List<Object> buildErrors(Capture capture) {
        List<Object> errors = new ArrayList<>();
        if (capture.failure != null) {
            String source = capture.isProduce()
                    ? ErrorMapper.SOURCE_ON_ERROR
                    : ErrorMapper.SOURCE_ON_EXCEPTION;
            errors.add(ErrorMapper.toError(capture.failure, source));
        }
        return errors;
    }

    /**
     * SDK-supplied Kafka coordinates first, then whatever the application attached via
     * {@code TreblleContext}, up to the schema's 20-property cap.
     */
    private Map<String, Object> buildMetadata(Capture capture) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        put(metadata, "kafka.operation", capture.operation);
        put(metadata, "kafka.topic", capture.topic);
        if (capture.partition >= 0) {
            put(metadata, "kafka.partition", capture.partition);
        }
        if (capture.offset >= 0) {
            put(metadata, "kafka.offset", capture.offset);
        }
        put(metadata, "kafka.key", capture.key);
        put(metadata, "kafka.client.id", capture.clientId);
        put(metadata, "kafka.group.id", capture.groupId);
        put(metadata, "kafka.timestamp.type", capture.timestampType);

        for (Map.Entry<String, Object> entry : nonNull(capture.metadata).entrySet()) {
            if (metadata.size() >= MAX_METADATA_ENTRIES) {
                break;
            }
            put(metadata, entry.getKey(), entry.getValue());
        }
        return metadata;
    }

    private static void put(Map<String, Object> metadata, String key, Object value) {
        if (key == null || value == null || metadata.size() >= MAX_METADATA_ENTRIES) {
            return;
        }
        String trimmedKey = key.length() > MAX_METADATA_KEY_LENGTH
                ? key.substring(0, MAX_METADATA_KEY_LENGTH)
                : key;
        if (trimmedKey.isEmpty()) {
            return;
        }
        if (value instanceof Number || value instanceof Boolean) {
            metadata.put(trimmedKey, value);
            return;
        }
        String text = value.toString();
        if (text.isEmpty()) {
            return;
        }
        metadata.put(trimmedKey, text.length() > MAX_METADATA_VALUE_LENGTH
                ? text.substring(0, MAX_METADATA_VALUE_LENGTH)
                : text);
    }

    /** {@code <clusterIdentity>:<topic>}, or just the topic when no cluster identity is known. */
    private String buildInternalId(Capture capture) {
        String clusterIdentity = isNotEmpty(capture.clusterName)
                ? capture.clusterName
                : capture.bootstrapServer;
        return isNotEmpty(clusterIdentity) ? clusterIdentity + ":" + capture.topic : capture.topic;
    }

    private String buildUrl(Capture capture) {
        String host = isNotEmpty(capture.bootstrapServer) ? capture.bootstrapServer : "kafka-cluster";
        return "kafka://" + host + "/" + (capture.topic == null ? "" : capture.topic);
    }

    private String buildUserAgent(Capture capture) {
        if (isNotEmpty(capture.clientId)) {
            return capture.clientId;
        }
        return "kafka-clients/" + kafkaClientVersion;
    }

    private static long size(Capture capture) {
        long total = 0;
        if (capture.serializedValueSize > 0) {
            total += capture.serializedValueSize;
        }
        if (capture.serializedKeySize > 0) {
            total += capture.serializedKeySize;
        }
        return total;
    }

    private static double round(double millis) {
        if (millis <= 0 || Double.isNaN(millis) || Double.isInfinite(millis)) {
            return 0;
        }
        return Math.round(millis * 1000.0) / 1000.0;
    }

    private static boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    private static <K, V> Map<K, V> nonNull(Map<K, V> map) {
        return map == null ? Collections.<K, V>emptyMap() : map;
    }
}
