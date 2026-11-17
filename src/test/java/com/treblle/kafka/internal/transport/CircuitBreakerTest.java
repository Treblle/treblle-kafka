package com.treblle.kafka.internal.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.treblle.kafka.internal.util.DebugLogger;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    private final AtomicLong clock = new AtomicLong(1_000_000_000L);
    private final CircuitBreaker breaker = new CircuitBreaker(new DebugLogger(false), clock::get);

    private void advanceMillis(long millis) {
        clock.addAndGet(millis * 1_000_000L);
    }

    @Test
    void startsNormal() {
        assertTrue(breaker.allowSend());
        assertTrue(breaker.allowSend());
        assertEquals(CircuitBreaker.State.NORMAL, breaker.state());
    }

    @Test
    void recoversAfterTwoSuccessfulProbes() {
        breaker.onFailure(null);
        assertFalse(breaker.allowSend());

        advanceMillis(CircuitBreaker.MAX_BACKOFF_MILLIS);
        assertTrue(breaker.allowSend(), "first probe");
        assertFalse(breaker.allowSend(), "only one probe at a time");
        breaker.onSuccess();

        assertTrue(breaker.allowSend(), "second probe");
        assertFalse(breaker.allowSend());
        breaker.onSuccess();

        assertEquals(CircuitBreaker.State.NORMAL, breaker.state());
        for (int i = 0; i < 10; i++) {
            assertTrue(breaker.allowSend());
        }
    }

    @Test
    void failedProbeBacksOffAgainWithLongerWindow() {
        breaker.onFailure(null);
        advanceMillis(CircuitBreaker.MAX_BACKOFF_MILLIS);
        assertTrue(breaker.allowSend());
        breaker.onFailure(null);

        assertTrue(breaker.isBackingOff());
        // The doubled window (>= 1.6s with jitter) has not expired after 1.2s.
        advanceMillis(1_200);
        assertFalse(breaker.allowSend());
    }

    @Test
    void lostProbeIsReplacedAfterTimeout() {
        breaker.onFailure(null);
        advanceMillis(CircuitBreaker.MAX_BACKOFF_MILLIS);
        assertTrue(breaker.allowSend());
        assertFalse(breaker.allowSend());

        advanceMillis(CircuitBreaker.PROBE_TIMEOUT_MILLIS);
        assertTrue(breaker.allowSend(), "a probe that never reported back must not wedge the breaker");
    }

    @Test
    void staleOutcomesWhileBackingOffAreIgnored() {
        breaker.onFailure(null);
        // Responses to requests sent before the breaker opened.
        breaker.onSuccess();
        breaker.onSuccess();
        breaker.onFailure(null);
        breaker.onFailure(null);

        assertTrue(breaker.isBackingOff());
        advanceMillis(1_300); // initial window is 1s +/-20%; stale failures did not escalate it
        assertTrue(breaker.allowSend());
    }

    @Test
    void neutralOutcomeFreesTheProbeSlotWithoutCounting() {
        breaker.onFailure(null);
        advanceMillis(CircuitBreaker.MAX_BACKOFF_MILLIS);
        assertTrue(breaker.allowSend());
        breaker.onNeutral();

        assertTrue(breaker.allowSend());
        breaker.onSuccess();
        assertEquals(CircuitBreaker.State.PROBING, breaker.state(), "neutral must not count");
        assertTrue(breaker.allowSend());
        breaker.onSuccess();
        assertEquals(CircuitBreaker.State.NORMAL, breaker.state());
    }

    @Test
    void honoursRetryAfterCappedAtMaximum() {
        breaker.onFailure(10L);
        advanceMillis(9_900);
        assertFalse(breaker.allowSend());
        advanceMillis(200);
        assertTrue(breaker.allowSend());

        CircuitBreaker other = new CircuitBreaker(new DebugLogger(false), clock::get);
        other.onFailure(86_400L);
        advanceMillis(CircuitBreaker.MAX_BACKOFF_MILLIS + 1);
        assertTrue(other.allowSend());
    }
}
