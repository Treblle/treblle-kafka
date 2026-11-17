package com.treblle.kafka;

import static com.treblle.kafka.FakeIngress.path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TreblleConsumerTrackerTest {

    private FakeIngress ingress;
    private Treblle treblle;
    private TreblleConsumerTracker tracker;

    @BeforeEach
    void setUp() throws Exception {
        ingress = new FakeIngress();
        treblle = Treblle.builder()
                .sdkToken("t")
                .apiKey("k")
                .ingressEndpoint(ingress.url())
                .excludedTopics("internal.*")
                .build();
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker-1:9092");
        props.put("group.id", "billing");
        tracker = treblle.consumerTracker(props);
    }

    @AfterEach
    void tearDown() {
        TreblleContext.clear();
        treblle.close();
        ingress.close();
    }

    private static ConsumerRecord<String, String> record(String topic, long timestamp) {
        return new ConsumerRecord<>(topic, 3, 42L, timestamp, TimestampType.CREATE_TIME,
                1, 12, "A-1", "{\"total\":10}", new RecordHeaders(), Optional.empty());
    }

    private List<Map<String, Object>> payloads(int expected) throws Exception {
        assertTrue(treblle.flush(Duration.ofSeconds(5)));
        assertEquals(expected, ingress.payloads().size(), "payloads: " + ingress.payloads());
        return ingress.payloads();
    }

    @Test
    void reportsConsumedRecordAndHandlerResult() throws Exception {
        String result = tracker.trackResult(record("orders.created", System.currentTimeMillis()),
                r -> "{\"ok\":true}");
        assertEquals("{\"ok\":true}", result);

        Map<String, Object> payload = payloads(1).get(0);
        assertEquals("GET", path(payload, "data", "request", "method"));
        assertEquals((Object) true, path(payload, "data", "response", "body", "ok"));
        assertEquals("billing", path(payload, "data", "response", "headers", "kafka-group-id"));
        assertEquals("42", path(payload, "data", "response", "headers", "kafka-offset"));
    }

    @Test
    void checkedExceptionsPropagateUnchangedAndAreReported() throws Exception {
        IOException failure = new IOException("disk full");
        IOException thrown = assertThrows(IOException.class, () ->
                tracker.track(record("orders.created", 1L), r -> {
                    throw failure;
                }));
        assertSame(failure, thrown);

        Map<String, Object> payload = payloads(1).get(0);
        assertEquals((Object) 500L, path(payload, "data", "response", "code"));
        List<Map<String, Object>> errors = path(payload, "data", "errors");
        assertEquals("onException", errors.get(0).get("source"));
    }

    @Test
    void handlerMetadataBelongsToTheConsumedRecordAndIsCleared() throws Exception {
        tracker.track(record("orders.created", 1L), r -> TreblleContext.put("tenant", "acme"));
        tracker.track(record("orders.created", 1L), r -> { });

        List<Map<String, Object>> payloads = payloads(2);
        assertEquals("acme", path(payloads.get(0), "data", "metadata", "tenant"));
        Map<String, Object> second = path(payloads.get(1), "data", "metadata");
        assertFalse(second.containsKey("tenant"));
    }

    @Test
    void metadataSetInExcludedHandlerDoesNotLeak() throws Exception {
        tracker.track(record("internal.audit", 1L), r -> TreblleContext.put("tenant", "acme"));
        tracker.track(record("orders.created", 1L), r -> { });
        Map<String, Object> metadata = path(payloads(1).get(0), "data", "metadata");
        assertFalse(metadata.containsKey("tenant"));
    }

    @Test
    void produceInsideHandlerSharesButDoesNotStealMetadata() throws Exception {
        MockProducer<String, String> mock =
                new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        Producer<String, String> producer = treblle.wrap(mock);

        tracker.track(record("orders.created", 1L), r -> {
            TreblleContext.put("tenant", "acme");
            producer.send(new ProducerRecord<>("orders.billed", 0, "A-1", "{}"));
        });

        List<Map<String, Object>> payloads = payloads(2);
        for (Map<String, Object> payload : payloads) {
            assertEquals("acme", path(payload, "data", "metadata", "tenant"),
                    "both the produce and the consume carry the handler's metadata");
        }
    }

    @Test
    void requestTimestampIsProcessingTimeNotRecordTime() throws Exception {
        tracker.track(record("orders.created", 1_000_000_000_000L), r -> { }); // year 2001
        Map<String, Object> payload = payloads(1).get(0);
        String timestamp = path(payload, "data", "request", "timestamp");
        assertFalse(timestamp.startsWith("2001"), timestamp);
        assertEquals("2001-09-09 01:46:40",
                path(payload, "data", "response", "headers", "kafka-timestamp"));
    }
}
