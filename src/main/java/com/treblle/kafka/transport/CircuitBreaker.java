package com.treblle.kafka.transport;

import com.treblle.kafka.util.DebugLogger;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stops the SDK hammering Ingress while Ingress is rejecting requests.
 *
 * <p>Three states:
 *
 * <ul>
 *   <li>{@code NORMAL} - everything is sent.
 *   <li>{@code BACKING_OFF} - everything is dropped until the backoff window expires. Payloads are
 *       <em>dropped</em>, never queued or delayed, so a Treblle outage can never build up work or
 *       memory in the host application.
 *   <li>{@code PROBING} - the window expired; exactly one payload is let through as a probe. Two
 *       consecutive successful probes return the breaker to {@code NORMAL}.
 * </ul>
 *
 * <p>State lives in a single {@link AtomicReference} to an immutable snapshot and is advanced with
 * compare-and-set, so {@link #allowSend()} is lock-free and costs one volatile read in the common
 * case.
 */
public final class CircuitBreaker {

    private static final long INITIAL_BACKOFF_MILLIS = 1_000L;
    private static final long MAX_BACKOFF_MILLIS = 60_000L;
    private static final int PROBES_TO_RECOVER = 2;

    /** If a probe never reports back, allow another one after this long rather than wedging. */
    private static final long PROBE_TIMEOUT_MILLIS = 30_000L;

    enum State {
        NORMAL,
        BACKING_OFF,
        PROBING
    }

    private static final class Snapshot {
        final State state;
        final long backoffMillis;
        final long openUntilNanos;
        final int consecutiveProbeSuccesses;

        Snapshot(State state, long backoffMillis, long openUntilNanos, int consecutiveProbeSuccesses) {
            this.state = state;
            this.backoffMillis = backoffMillis;
            this.openUntilNanos = openUntilNanos;
            this.consecutiveProbeSuccesses = consecutiveProbeSuccesses;
        }
    }

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(
            new Snapshot(State.NORMAL, INITIAL_BACKOFF_MILLIS, 0L, 0));

    private final DebugLogger logger;

    public CircuitBreaker(DebugLogger logger) {
        this.logger = logger;
    }

    /**
     * Returns true when this payload may be sent. Called on the hot path - must stay O(1) and must
     * never block.
     */
    public boolean allowSend() {
        while (true) {
            Snapshot current = snapshot.get();
            if (current.state == State.NORMAL) {
                return true;
            }

            long now = System.nanoTime();
            if (current.state == State.BACKING_OFF) {
                if (now < current.openUntilNanos) {
                    return false;
                }
                // Window expired: promote to PROBING and let exactly this caller through.
                Snapshot probing = new Snapshot(
                        State.PROBING,
                        current.backoffMillis,
                        now + millisToNanos(PROBE_TIMEOUT_MILLIS),
                        current.consecutiveProbeSuccesses);
                if (snapshot.compareAndSet(current, probing)) {
                    logger.log("circuit breaker probing after "
                            + current.backoffMillis + "ms backoff");
                    return true;
                }
                continue;
            }

            // PROBING: a probe is already in flight. Only let another through if it went missing.
            if (now < current.openUntilNanos) {
                return false;
            }
            Snapshot retry = new Snapshot(
                    State.PROBING,
                    current.backoffMillis,
                    now + millisToNanos(PROBE_TIMEOUT_MILLIS),
                    0);
            if (snapshot.compareAndSet(current, retry)) {
                return true;
            }
        }
    }

    /** Records a 2xx/3xx response from Ingress. */
    public void onSuccess() {
        while (true) {
            Snapshot current = snapshot.get();
            if (current.state == State.NORMAL) {
                return;
            }
            int successes = current.consecutiveProbeSuccesses + 1;
            if (successes >= PROBES_TO_RECOVER) {
                Snapshot recovered =
                        new Snapshot(State.NORMAL, INITIAL_BACKOFF_MILLIS, 0L, 0);
                if (snapshot.compareAndSet(current, recovered)) {
                    logger.log("circuit breaker recovered; resuming normal sending");
                    return;
                }
                continue;
            }
            // One more probe needed. Make the next caller eligible immediately.
            Snapshot probing =
                    new Snapshot(State.PROBING, current.backoffMillis, System.nanoTime(), successes);
            if (snapshot.compareAndSet(current, probing)) {
                logger.log("circuit breaker probe succeeded (" + successes + "/"
                        + PROBES_TO_RECOVER + ")");
                return;
            }
        }
    }

    /**
     * Records a 4xx/5xx response or a transport failure.
     *
     * @param retryAfterSeconds the {@code Retry-After} value from a 429, or null. This is the only
     *     response header the SDK listens to.
     */
    public void onFailure(Long retryAfterSeconds) {
        while (true) {
            Snapshot current = snapshot.get();
            long backoffMillis = nextBackoffMillis(current, retryAfterSeconds);
            Snapshot backingOff = new Snapshot(
                    State.BACKING_OFF,
                    backoffMillis,
                    System.nanoTime() + millisToNanos(backoffMillis),
                    0);
            if (snapshot.compareAndSet(current, backingOff)) {
                if (current.state != State.BACKING_OFF) {
                    logger.warn("circuit breaker opened; dropping payloads for "
                            + backoffMillis + "ms");
                }
                return;
            }
        }
    }

    /** True when payloads are currently being dropped. For debug logging only. */
    public boolean isBackingOff() {
        return snapshot.get().state == State.BACKING_OFF;
    }

    private static long nextBackoffMillis(Snapshot current, Long retryAfterSeconds) {
        if (retryAfterSeconds != null && retryAfterSeconds >= 0) {
            // Honour the server, capped so a hostile or mistaken header cannot mute the SDK.
            return Math.min(retryAfterSeconds * 1000L, MAX_BACKOFF_MILLIS);
        }
        long base = current.state == State.NORMAL
                ? INITIAL_BACKOFF_MILLIS
                : Math.min(current.backoffMillis * 2, MAX_BACKOFF_MILLIS);
        return applyJitter(base);
    }

    /** +/-20% jitter so many producers backing off together do not retry in lockstep. */
    private static long applyJitter(long millis) {
        double factor = 0.8 + (ThreadLocalRandom.current().nextDouble() * 0.4);
        long jittered = Math.round(millis * factor);
        return Math.max(1L, Math.min(jittered, MAX_BACKOFF_MILLIS));
    }

    private static long millisToNanos(long millis) {
        return millis * 1_000_000L;
    }
}
