package com.treblle.kafka.internal.transport;

import com.treblle.kafka.internal.util.DebugLogger;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

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
 *   <li>{@code PROBING} - the window expired; one payload at a time is let through as a probe. Two
 *       consecutive successful probes return the breaker to {@code NORMAL}; a failed probe sends it
 *       back to {@code BACKING_OFF} with a longer window.
 * </ul>
 *
 * <p>Responses to requests sent <em>before</em> the breaker opened can still arrive afterwards.
 * They say nothing about the current state of Ingress, so outcomes reported while
 * {@code BACKING_OFF} are ignored, and a success reported while {@code NORMAL} is a no-op.
 *
 * <p>State lives in a single {@link AtomicReference} to an immutable snapshot and is advanced with
 * compare-and-set, so {@link #allowSend()} is lock-free and costs one volatile read in the common
 * case.
 */
public final class CircuitBreaker {

    static final long INITIAL_BACKOFF_MILLIS = 1_000L;
    static final long MAX_BACKOFF_MILLIS = 60_000L;
    static final int PROBES_TO_RECOVER = 2;

    /** If a probe never reports back, allow another one after this long rather than wedging. */
    static final long PROBE_TIMEOUT_MILLIS = 30_000L;

    enum State {
        NORMAL,
        BACKING_OFF,
        PROBING
    }

    private static final class Snapshot {
        final State state;
        final long backoffMillis;
        /** BACKING_OFF: when the window ends. PROBING: when an in-flight probe counts as lost. */
        final long deadlineNanos;
        final int consecutiveProbeSuccesses;
        final boolean probeInFlight;

        Snapshot(State state, long backoffMillis, long deadlineNanos,
                int consecutiveProbeSuccesses, boolean probeInFlight) {
            this.state = state;
            this.backoffMillis = backoffMillis;
            this.deadlineNanos = deadlineNanos;
            this.consecutiveProbeSuccesses = consecutiveProbeSuccesses;
            this.probeInFlight = probeInFlight;
        }
    }

    private static final Snapshot NORMAL =
            new Snapshot(State.NORMAL, INITIAL_BACKOFF_MILLIS, 0L, 0, false);

    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(NORMAL);
    private final DebugLogger logger;
    private final LongSupplier nanoClock;

    public CircuitBreaker(DebugLogger logger) {
        this(logger, System::nanoTime);
    }

    CircuitBreaker(DebugLogger logger, LongSupplier nanoClock) {
        this.logger = logger;
        this.nanoClock = nanoClock;
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

            long now = nanoClock.getAsLong();
            if (current.state == State.BACKING_OFF) {
                if (now - current.deadlineNanos < 0) {
                    return false;
                }
                // Window expired: promote to PROBING and let exactly this caller through.
                if (snapshot.compareAndSet(current, probe(current, now, 0))) {
                    logger.log("circuit breaker probing after "
                            + current.backoffMillis + "ms backoff");
                    return true;
                }
                continue;
            }

            // PROBING.
            if (!current.probeInFlight) {
                // The previous probe succeeded and asked for another. Keep its success count.
                if (snapshot.compareAndSet(
                        current, probe(current, now, current.consecutiveProbeSuccesses))) {
                    return true;
                }
                continue;
            }
            if (now - current.deadlineNanos < 0) {
                return false;
            }
            // The in-flight probe never reported back. Count it as lost and start over.
            if (snapshot.compareAndSet(current, probe(current, now, 0))) {
                return true;
            }
        }
    }

    /** Records a 2xx/3xx response from Ingress. */
    public void onSuccess() {
        while (true) {
            Snapshot current = snapshot.get();
            if (current.state != State.PROBING) {
                // NORMAL: nothing to do. BACKING_OFF: a stale response from before the breaker
                // opened - it must not cut the backoff window short.
                return;
            }
            int successes = current.consecutiveProbeSuccesses + 1;
            Snapshot next = successes >= PROBES_TO_RECOVER
                    ? NORMAL
                    : new Snapshot(State.PROBING, current.backoffMillis, 0L, successes, false);
            if (snapshot.compareAndSet(current, next)) {
                if (next == NORMAL) {
                    logger.log("circuit breaker recovered; resuming normal sending");
                } else {
                    logger.log("circuit breaker probe succeeded (" + successes + "/"
                            + PROBES_TO_RECOVER + ")");
                }
                return;
            }
        }
    }

    /**
     * Records a response that says nothing about Ingress health - e.g. a 400 or 413 caused by one
     * specific payload. Frees the probe slot without counting towards recovery.
     */
    public void onNeutral() {
        while (true) {
            Snapshot current = snapshot.get();
            if (current.state != State.PROBING || !current.probeInFlight) {
                return;
            }
            Snapshot next = new Snapshot(State.PROBING, current.backoffMillis, 0L,
                    current.consecutiveProbeSuccesses, false);
            if (snapshot.compareAndSet(current, next)) {
                return;
            }
        }
    }

    /**
     * Records a 5xx/429/401/403 response or a transport failure.
     *
     * @param retryAfterSeconds the {@code Retry-After} value from a 429, or null. This is the only
     *     response header the SDK listens to.
     */
    public void onFailure(Long retryAfterSeconds) {
        while (true) {
            Snapshot current = snapshot.get();
            long now = nanoClock.getAsLong();
            Snapshot next;
            if (current.state == State.BACKING_OFF) {
                // Stale failures from requests sent before the breaker opened. Only an explicit
                // Retry-After may extend the window; nothing else escalates the backoff.
                if (retryAfterSeconds == null) {
                    return;
                }
                long requested = now + millisToNanos(capped(retryAfterSeconds));
                if (requested - current.deadlineNanos <= 0) {
                    return;
                }
                next = new Snapshot(State.BACKING_OFF, current.backoffMillis, requested, 0, false);
            } else {
                long backoffMillis = nextBackoffMillis(current, retryAfterSeconds);
                next = new Snapshot(State.BACKING_OFF, backoffMillis,
                        now + millisToNanos(backoffMillis), 0, false);
            }
            if (snapshot.compareAndSet(current, next)) {
                if (current.state == State.NORMAL) {
                    logger.warn("circuit breaker opened; dropping payloads for "
                            + next.backoffMillis + "ms");
                }
                return;
            }
        }
    }

    /** True when payloads are currently being dropped. For debug logging and tests. */
    public boolean isBackingOff() {
        return snapshot.get().state == State.BACKING_OFF;
    }

    State state() {
        return snapshot.get().state;
    }

    private Snapshot probe(Snapshot current, long now, int successes) {
        return new Snapshot(State.PROBING, current.backoffMillis,
                now + millisToNanos(PROBE_TIMEOUT_MILLIS), successes, true);
    }

    private static long nextBackoffMillis(Snapshot current, Long retryAfterSeconds) {
        if (retryAfterSeconds != null && retryAfterSeconds >= 0) {
            return capped(retryAfterSeconds);
        }
        long base = current.state == State.NORMAL
                ? INITIAL_BACKOFF_MILLIS
                : Math.min(current.backoffMillis * 2, MAX_BACKOFF_MILLIS);
        return applyJitter(base);
    }

    /** Honour the server, capped so a hostile or mistaken header cannot mute the SDK. */
    private static long capped(long retryAfterSeconds) {
        return Math.min(Math.max(0L, retryAfterSeconds), MAX_BACKOFF_MILLIS / 1000L) * 1000L;
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
