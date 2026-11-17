package com.treblle.kafka.internal.transport;

import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.internal.util.DebugLogger;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

/**
 * Ships payloads to Treblle Ingress.
 *
 * <p>Requests are asynchronous, but the number in flight is capped at {@link #MAX_IN_FLIGHT}. When
 * every slot is taken, {@link #send} blocks the dispatcher's worker thread (never an application
 * thread) until one frees up, so a slow Ingress pushes back on the bounded dispatch queue instead
 * of piling up requests in memory. Every request has a timeout, so a slot is always released.
 *
 * <p>The status code and {@code Retry-After} header are observed only to drive the
 * {@link CircuitBreaker} and the debug log. There are no retries - a dropped payload is always
 * preferable to work piling up in the host application.
 */
final class IngressClient {

    static final int MAX_IN_FLIGHT = 8;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);

    private final TreblleConfig config;
    private final DebugLogger logger;
    private final CircuitBreaker breaker;
    private final HttpClient httpClient;
    private final URI endpoint;
    private final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
    private final AtomicBoolean credentialsWarningLogged = new AtomicBoolean();

    IngressClient(TreblleConfig config, DebugLogger logger, CircuitBreaker breaker, Executor executor) {
        this.config = config;
        this.logger = logger;
        this.breaker = breaker;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NEVER)
                .executor(executor)
                .build();
        this.endpoint = resolveEndpoint(config.getIngressEndpoint(), logger);
    }

    /** True when the configured endpoint is a usable URL. */
    boolean isUsable() {
        return endpoint != null;
    }

    /**
     * GZIPs and POSTs {@code json}. Blocks the calling (worker) thread only while all
     * {@link #MAX_IN_FLIGHT} slots are taken; the response is handled in the background purely to
     * feed the circuit breaker.
     *
     * @throws InterruptedException if the worker is interrupted while waiting for a slot
     */
    void send(String json) throws InterruptedException {
        if (endpoint == null) {
            return;
        }
        byte[] compressed;
        try {
            compressed = gzip(json);
        } catch (Exception e) {
            logger.error("failed to compress payload; dropping it", e);
            return;
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .header("x-api-key", config.getSdkToken())
                .POST(HttpRequest.BodyPublishers.ofByteArray(compressed))
                .build();

        inFlight.acquire();
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .whenComplete((response, failure) -> {
                        inFlight.release();
                        observe(response, failure);
                    });
        } catch (Exception e) {
            // A rejected execution or a malformed request must never surface to the host app.
            inFlight.release();
            logger.error("failed to dispatch payload", e);
            breaker.onFailure(null);
        }
    }

    /** True when no request is in flight. */
    boolean isIdle() {
        return inFlight.availablePermits() == MAX_IN_FLIGHT;
    }

    /**
     * Waits up to {@code timeoutNanos} for every in-flight request to complete.
     *
     * @return true if all requests completed in time
     */
    boolean awaitIdle(long timeoutNanos) throws InterruptedException {
        if (!inFlight.tryAcquire(MAX_IN_FLIGHT, Math.max(0L, timeoutNanos), TimeUnit.NANOSECONDS)) {
            return false;
        }
        inFlight.release(MAX_IN_FLIGHT);
        return true;
    }

    private void observe(HttpResponse<Void> response, Throwable failure) {
        try {
            if (failure != null) {
                logger.error("send to Treblle failed", failure);
                breaker.onFailure(null);
                return;
            }
            int status = response.statusCode();
            if (status >= 200 && status < 400) {
                logger.log("payload accepted by Treblle (HTTP " + status + ")");
                breaker.onSuccess();
                return;
            }
            if (status == 401 || status == 403) {
                // Every payload will be rejected the same way, so back off like an outage - and
                // say so even without debug mode, because otherwise nothing ever shows up.
                if (credentialsWarningLogged.compareAndSet(false, true)) {
                    logger.alwaysWarn("Treblle rejected the SDK credentials (HTTP " + status
                            + "). Check sdkToken and apiKey. This is logged once.");
                }
                breaker.onFailure(null);
                return;
            }
            if (status == 429 || status >= 500) {
                Long retryAfter = status == 429 ? parseRetryAfter(response) : null;
                logger.warn("Treblle responded HTTP " + status
                        + (retryAfter == null ? "" : " with Retry-After: " + retryAfter + "s"));
                breaker.onFailure(retryAfter);
                return;
            }
            // Any other 4xx (400, 413, 422, ...) is about this one payload, not about Ingress.
            // Dropping it is enough; it must not stop every other payload being sent.
            logger.warn("Treblle rejected a payload (HTTP " + status + "); dropped it");
            breaker.onNeutral();
        } catch (RuntimeException e) {
            logger.error("failed to handle Treblle response", e);
        }
    }

    /**
     * Parses only the delta-seconds form of {@code Retry-After}. The HTTP-date form falls through
     * to null, which puts the breaker on its normal exponential backoff.
     */
    private static Long parseRetryAfter(HttpResponse<Void> response) {
        Optional<String> header = response.headers().firstValue("Retry-After");
        if (!header.isPresent()) {
            return null;
        }
        try {
            long seconds = Long.parseLong(header.get().trim());
            return seconds >= 0 ? seconds : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static byte[] gzip(String json) throws java.io.IOException {
        byte[] raw = json.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(Math.max(64, raw.length / 4));
        try (GZIPOutputStream gzip = new GZIPOutputStream(buffer)) {
            gzip.write(raw);
        }
        return buffer.toByteArray();
    }

    /**
     * Accepts {@code https} anywhere, and plain {@code http} only for loopback hosts (local
     * development and tests). The SDK token travels in every request, so it must never cross a
     * network unencrypted.
     */
    static URI resolveEndpoint(String value, DebugLogger logger) {
        try {
            URI uri = URI.create(value);
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
            if (scheme.equals("https") && uri.getHost() != null) {
                return uri;
            }
            if (scheme.equals("http") && isLoopback(uri.getHost())) {
                return uri;
            }
            logger.alwaysWarn("ingressEndpoint '" + value + "' is not an https URL (plain http is "
                    + "only allowed for localhost); the SDK will not send anything");
            return null;
        } catch (RuntimeException e) {
            logger.alwaysWarn("ingressEndpoint '" + value
                    + "' is not a valid URL; the SDK will not send anything");
            return null;
        }
    }

    private static boolean isLoopback(String host) {
        if (host == null) {
            return false;
        }
        String h = host.toLowerCase(Locale.ROOT);
        return h.equals("localhost") || h.equals("127.0.0.1") || h.equals("[::1]") || h.equals("::1");
    }
}
