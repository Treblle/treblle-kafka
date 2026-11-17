package com.treblle.kafka.internal.core;

import static com.treblle.kafka.FakeIngress.path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.ValueRenderer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PayloadBuilderTest {

    private static PayloadBuilder builder(ValueRenderer renderer, String... extraKeywords) {
        TreblleConfig config = TreblleConfig.builder()
                .sdkToken("t")
                .apiKey("k")
                .maskedKeywords(extraKeywords)
                .valueRenderer(renderer)
                .build();
        return new PayloadBuilder(config, new Masker(config.getMaskedKeywords()), "3.9.0");
    }

    private static Capture produce(Object value) {
        Capture capture = new Capture();
        capture.operation = Capture.OPERATION_PRODUCE;
        capture.topic = "orders.created";
        capture.value = value;
        capture.operationTimestampMillis = 1_700_000_000_000L;
        capture.recordTimestampMillis = 1_700_000_000_000L;
        return capture;
    }

    @Test
    void masksDefaultKeywordsWithoutAnyConfiguration() {
        Map<String, Object> payload = builder(null).build(produce(
                "{\"orderId\":\"A-1\",\"password\":\"hunter2\",\"card_number\":\"4111\"}"));
        Map<String, Object> body = path(payload, "data", "request", "body");
        assertEquals("A-1", body.get("orderId"));
        assertEquals("*******", body.get("password"));
        assertEquals("****", body.get("card_number"));
    }

    @Test
    void configuredKeywordsAddToDefaults() {
        Map<String, Object> payload = builder(null, "email").build(produce(
                "{\"email\":\"ada@example.com\",\"password\":\"x\"}"));
        Map<String, Object> body = path(payload, "data", "request", "body");
        assertEquals("***************", body.get("email"));
        assertEquals("*", body.get("password"));
    }

    @Test
    void masksSecretsInsideErrorMessages() {
        Capture capture = produce("{}");
        capture.failure = new IllegalStateException("login failed for password=hunter2, retry");
        Map<String, Object> payload = builder(null).build(capture);

        List<Map<String, Object>> errors = path(payload, "data", "errors");
        String message = (String) errors.get(0).get("message");
        assertFalse(message.contains("hunter2"), message);
        assertTrue(message.contains("password=*******"), message);

        Map<String, Object> responseBody = path(payload, "data", "response", "body");
        assertFalse(responseBody.get("message").toString().contains("hunter2"));
    }

    @Test
    void masksMetadataByKey() {
        Capture capture = produce("{}");
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("tenant", "acme");
        metadata.put("api_key", "sk_live_123");
        capture.metadata = metadata;
        Map<String, Object> payload = builder(null).build(capture);

        Map<String, Object> sent = path(payload, "data", "metadata");
        assertEquals("acme", sent.get("tenant"));
        assertEquals("***********", sent.get("api_key"));
    }

    @Test
    void consumeUsesOperationTimeAndReportsRecordTimeSeparately() {
        Capture capture = produce("{}");
        capture.operation = Capture.OPERATION_CONSUME;
        capture.recordTimestampMillis = 1_600_000_000_000L; // produced long ago
        capture.operationTimestampMillis = 1_700_000_000_000L;
        Map<String, Object> payload = builder(null).build(capture);

        assertEquals("2023-11-14 22:13:20", path(payload, "data", "request", "timestamp"));
        assertEquals("2020-09-13 12:26:40",
                path(payload, "data", "response", "headers", "kafka-timestamp"));
    }

    @Test
    void usesValueRendererForPojos() {
        ValueRenderer renderer = value -> "{\"rendered\":\"" + ((Order) value).id + "\"}";
        Map<String, Object> payload = builder(renderer).build(produce(new Order("A-7")));
        Map<String, Object> body = path(payload, "data", "request", "body");
        assertEquals("A-7", body.get("rendered"));
    }

    @Test
    void failingRendererIsContained() {
        ValueRenderer renderer = value -> {
            throw new Exception("boom");
        };
        Map<String, Object> payload = builder(renderer).build(produce(new Order("A-7")));
        Map<String, Object> body = path(payload, "data", "request", "body");
        assertTrue(body.get("message").toString().contains("valueRenderer"));
    }

    @Test
    void mapBodiesAreCopiedAndMasked() {
        Map<String, Object> value = new HashMap<>();
        value.put("secret", "s3cr3t");
        value.put("nested", new HashMap<>(Map.of("token", "abc")));
        Map<String, Object> payload = builder(null).build(produce(value));
        Map<String, Object> body = path(payload, "data", "request", "body");
        assertEquals("******", body.get("secret"));
        assertEquals("***", ((Map<?, ?>) body.get("nested")).get("token"));
        assertEquals("s3cr3t", value.get("secret"), "the application's object is never modified");
    }

    @Test
    void internalIdIncludesClusterAndTopic() {
        Capture capture = produce("{}");
        capture.clusterName = "prod-eu";
        Map<String, Object> payload = builder(null).build(capture);
        assertEquals("prod-eu:orders.created", payload.get("internal_id"));
        assertEquals("orders.created", payload.get("internal_name"));
    }

    static final class Order {
        final String id;

        Order(String id) {
            this.id = id;
        }
    }
}
