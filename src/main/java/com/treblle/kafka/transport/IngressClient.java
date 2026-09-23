package com.treblle.kafka.transport;

import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.util.DebugLogger;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

/**
 * Ships payloads to Treblle Ingress.
 *
 * <p>Fire-and-forget by construction: {@code sendAsync} is never awaited and the response body is
 * discarded. The status code and {@code Retry-After} header are observed only to drive the
 * {@link CircuitBreaker} and the debug log. There are no retries - a dropped payload is always
 * preferable to work piling up in the host application.
 *
 * <p>One {@link HttpClient} is shared for the life of the SDK so connections are pooled and kept
 * alive across a high-traffic producer's sends.
 */
final class IngressClient {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);

    private final TreblleConfig config;
    private final DebugLogger logger;
    private final CircuitBreaker breaker;
    private final HttpClient httpClient;
    private final URI endpoint;

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
     * GZIPs and POSTs {@code json}. Returns immediately; the response is handled in the background
     * purely to feed the circuit breaker.
     */
    void send(String json) {
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

        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .whenComplete(this::observe);
        } catch (Exception e) {
            // A rejected execution or a malformed request must never surface to the host app.
            logger.error("failed to dispatch payload", e);
            breaker.onFailure(null);
        }
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
            Long retryAfter = status == 429 ? parseRetryAfter(response) : null;
            logger.warn("Treblle responded HTTP " + status
                    + (retryAfter == null ? "" : " with Retry-After: " + retryAfter + "s"));
            breaker.onFailure(retryAfter);
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

    private static URI resolveEndpoint(String value, DebugLogger logger) {
        try {
            URI uri = URI.create(value);
            String scheme = uri.getScheme();
            if (scheme == null || !(scheme.equals("http") || scheme.equals("https"))) {
                logger.warn("ingressEndpoint '" + value + "' is not an http(s) URL; disabling sends");
                return null;
            }
            return uri;
        } catch (RuntimeException e) {
            logger.warn("ingressEndpoint '" + value + "' is not a valid URL; disabling sends");
            return null;
        }
    }
}
