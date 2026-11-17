package com.treblle.kafka.internal.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.treblle.kafka.FakeIngress;
import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.internal.core.Capture;
import com.treblle.kafka.internal.util.DebugLogger;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DispatcherTest {

    private FakeIngress ingress;
    private Dispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        ingress = new FakeIngress();
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.close(1, TimeUnit.SECONDS);
        }
        ingress.close();
    }

    private Dispatcher dispatcher() {
        TreblleConfig config = TreblleConfig.builder()
                .sdkToken("token-123")
                .apiKey("key-456")
                .ingressEndpoint(ingress.url())
                .build();
        dispatcher = new Dispatcher(config, new DebugLogger(false), "3.9.0");
        return dispatcher;
    }

    private static Capture capture(String topic) {
        Capture capture = new Capture();
        capture.operation = Capture.OPERATION_PRODUCE;
        capture.topic = topic;
        capture.value = "{\"id\":1}";
        capture.estimatedBytes = 8;
        return capture;
    }

    @Test
    void sendsGzippedPayloadWithSdkTokenHeader() throws Exception {
        assertTrue(dispatcher().submit(capture("orders")));
        assertEquals(1, ingress.await(1, Duration.ofSeconds(5)).size());
        assertEquals("token-123", ingress.apiKeyHeaders().get(0));
        assertEquals("key-456", ingress.payloads().get(0).get("api_key"));
    }

    @Test
    void capsConcurrentRequestsToIngress() throws Exception {
        ingress.delay(150);
        Dispatcher d = dispatcher();
        for (int i = 0; i < 40; i++) {
            d.submit(capture("orders"));
        }
        assertTrue(d.flush(20, TimeUnit.SECONDS));
        assertEquals(40, ingress.payloads().size());
        assertTrue(ingress.maxConcurrent() <= IngressClient.MAX_IN_FLIGHT,
                "max concurrent was " + ingress.maxConcurrent());
    }

    @Test
    void closeFlushesWhatIsQueued() throws Exception {
        ingress.delay(20);
        Dispatcher d = dispatcher();
        for (int i = 0; i < 25; i++) {
            d.submit(capture("orders"));
        }
        d.close(10, TimeUnit.SECONDS);
        assertEquals(25, ingress.payloads().size());
        assertFalse(d.submit(capture("orders")), "closed dispatcher accepts nothing");
    }

    @Test
    void enforcesMemoryBudget() {
        ingress.delay(2_000);
        Dispatcher d = dispatcher();
        Capture big = capture("orders");
        big.estimatedBytes = Dispatcher.MAX_QUEUED_BYTES - 10;
        Capture next = capture("orders");
        next.estimatedBytes = 100;

        // The first may be picked up by the worker at once; keep submitting until the budget bites.
        boolean rejected = false;
        for (int i = 0; i < 20 && !rejected; i++) {
            d.submit(big);
            rejected = !d.submit(next);
        }
        assertTrue(rejected, "the queue must refuse captures over its memory budget");
    }

    @Test
    void rejectsPlainHttpForNonLoopbackHosts() {
        DebugLogger logger = new DebugLogger(false);
        assertNull(IngressClient.resolveEndpoint("http://ingress.example.com", logger));
        assertNull(IngressClient.resolveEndpoint("ftp://ingress.example.com", logger));
        assertNull(IngressClient.resolveEndpoint("not a url", logger));
        assertNotNull(IngressClient.resolveEndpoint("https://ingress.treblle.com", logger));
        assertNotNull(IngressClient.resolveEndpoint("http://localhost:8080", logger));
        assertNotNull(IngressClient.resolveEndpoint("http://127.0.0.1:8080", logger));
    }

    @Test
    void payloadRejectionDoesNotStopOtherPayloads() throws Exception {
        ingress.respondWith(413);
        Dispatcher d = dispatcher();
        d.submit(capture("orders"));
        assertTrue(d.flush(5, TimeUnit.SECONDS));
        ingress.respondWith(200);
        assertTrue(d.submit(capture("orders")), "a 413 must not open the circuit breaker");
    }
}
