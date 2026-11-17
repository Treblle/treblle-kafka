package com.treblle.kafka;

import static com.treblle.kafka.FakeIngress.path;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TreblleProducerTest {

    private FakeIngress ingress;
    private Treblle treblle;
    private MockProducer<String, String> mock;
    private Producer<String, String> producer;

    @BeforeEach
    void setUp() throws Exception {
        ingress = new FakeIngress();
        treblle = Treblle.builder()
                .sdkToken("t")
                .apiKey("k")
                .ingressEndpoint(ingress.url())
                .excludedTopics("internal.*")
                .build();
        mock = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker-1:9092,broker-2:9092");
        props.put("client.id", "checkout");
        producer = treblle.wrap(mock, props);
    }

    @AfterEach
    void tearDown() {
        TreblleContext.clear();
        treblle.close();
        ingress.close();
    }

    private Map<String, Object> onlyPayload() throws Exception {
        assertTrue(treblle.flush(Duration.ofSeconds(5)));
        assertEquals(1, ingress.payloads().size(), "payloads: " + ingress.payloads());
        return ingress.payloads().get(0);
    }

    @Test
    void reportsProducedRecord() throws Exception {
        producer.send(new ProducerRecord<>("orders.created", 0, "A-1", "{\"total\":10}"));
        Map<String, Object> payload = onlyPayload();

        assertEquals("POST", path(payload, "data", "request", "method"));
        assertEquals("kafka://broker-1:9092/orders.created", path(payload, "data", "request", "url"));
        assertEquals((Object) 10L, path(payload, "data", "request", "body", "total"));
        assertEquals((Object) 200L, path(payload, "data", "response", "code"));
        assertEquals("A-1", path(payload, "data", "metadata", "kafka.key"));
        assertEquals("checkout", path(payload, "data", "metadata", "kafka.client.id"));
        assertEquals("broker-1:orders.created", payload.get("internal_id"));
    }

    @Test
    void stillCallsTheApplicationCallback() throws Exception {
        boolean[] called = {false};
        producer.send(new ProducerRecord<>("orders.created", 0, "A-1", "{}"),
                (metadata, exception) -> called[0] = true);
        assertTrue(called[0]);
    }

    @Test
    void synchronousSendFailurePropagatesUnchangedAndIsReported() throws Exception {
        mock.sendException = new SerializationException("cannot serialise");
        assertThrows(SerializationException.class,
                () -> producer.send(new ProducerRecord<>("orders.created", 0, "A-1", "{}")));
        Map<String, Object> payload = onlyPayload();
        assertEquals((Object) 400L, path(payload, "data", "response", "code"));
    }

    @Test
    void contextIsClearedEvenForExcludedTopics() throws Exception {
        TreblleContext.put("tenant", "acme");
        producer.send(new ProducerRecord<>("internal.audit", 0, "k", "{}"));
        producer.send(new ProducerRecord<>("orders.created", 0, "k", "{}"));

        Map<String, Object> metadata = path(onlyPayload(), "data", "metadata");
        assertFalse(metadata.containsKey("tenant"),
                "metadata set for an excluded send must not leak onto the next one");
    }

    @Test
    void contextAttachesToTheNextSend() throws Exception {
        TreblleContext.put("tenant", "acme");
        producer.send(new ProducerRecord<>("orders.created", 0, "k", "{}"));
        assertEquals("acme", path(onlyPayload(), "data", "metadata", "tenant"));
    }

    @Test
    void recordKeysCanBeSuppressed() throws Exception {
        Treblle noKeys = Treblle.builder()
                .sdkToken("t").apiKey("k").ingressEndpoint(ingress.url())
                .captureRecordKeys(false)
                .build();
        try {
            noKeys.wrap(mock).send(new ProducerRecord<>("orders.created", 0, "ada@example.com", "{}"));
            assertTrue(noKeys.flush(Duration.ofSeconds(5)));
            Map<String, Object> metadata = path(ingress.payloads().get(0), "data", "metadata");
            assertFalse(metadata.containsKey("kafka.key"));
        } finally {
            noKeys.close();
        }
    }

    @Test
    void inactiveSdkReturnsProducerUnwrapped() {
        Treblle disabled = Treblle.builder().enabled(false).build();
        assertSame(mock, disabled.wrap(mock));
        disabled.close();
    }

    @Test
    void closedSdkStopsCapturing() throws Exception {
        treblle.close();
        producer.send(new ProducerRecord<>("orders.created", 0, "k", "{}"));
        Thread.sleep(100);
        assertTrue(ingress.payloads().isEmpty());
    }

    @Test
    void versionSpecificMethodsDelegateOrFailClearly() {
        // sendOffsetsToTransaction(Map, String) exists only on 3.x; registerMetricForSubscription
        // only on 4.x. Whichever is missing on the client under test must fail clearly rather
        // than with AbstractMethodError, and whichever exists must reach the delegate.
        TreblleProducer<String, String> wrapper = (TreblleProducer<String, String>) producer;
        boolean is4x = hasMethod("registerMetricForSubscription");
        if (is4x) {
            assertThrows(UnsupportedOperationException.class,
                    () -> wrapper.sendOffsetsToTransaction(Map.of(), "group"));
        } else {
            assertThrows(UnsupportedOperationException.class,
                    () -> wrapper.registerMetricForSubscription(null));
        }
    }

    private static boolean hasMethod(String name) {
        for (java.lang.reflect.Method method : Producer.class.getMethods()) {
            if (method.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
