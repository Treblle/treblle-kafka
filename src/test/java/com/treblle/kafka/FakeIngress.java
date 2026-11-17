package com.treblle.kafka;

import com.sun.net.httpserver.HttpServer;
import com.treblle.kafka.internal.json.JsonParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/** A local stand-in for Treblle Ingress that records every payload it receives. */
public final class FakeIngress implements AutoCloseable {

    private final HttpServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<Map<String, Object>> payloads = new CopyOnWriteArrayList<>();
    private final List<String> apiKeyHeaders = new CopyOnWriteArrayList<>();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();

    private volatile int status = 200;
    private volatile long delayMillis;

    public FakeIngress() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            int now = concurrent.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            try {
                byte[] body = readGzip(exchange.getRequestBody());
                apiKeyHeaders.add(exchange.getRequestHeaders().getFirst("x-api-key"));
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = (Map<String, Object>)
                        JsonParser.parse(new String(body, StandardCharsets.UTF_8));
                if (delayMillis > 0) {
                    Thread.sleep(delayMillis);
                }
                payloads.add(payload);
                exchange.sendResponseHeaders(status, -1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                concurrent.decrementAndGet();
                exchange.close();
            }
        });
        server.start();
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    }

    public void respondWith(int status) {
        this.status = status;
    }

    public void delay(long millis) {
        this.delayMillis = millis;
    }

    public List<Map<String, Object>> payloads() {
        return payloads;
    }

    public List<String> apiKeyHeaders() {
        return apiKeyHeaders;
    }

    public int maxConcurrent() {
        return maxConcurrent.get();
    }

    /** Waits until at least {@code count} payloads have arrived. */
    public List<Map<String, Object>> await(int count, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (payloads.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        return payloads;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private static byte[] readGzip(InputStream raw) throws IOException {
        try (InputStream in = new GZIPInputStream(raw)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            in.transferTo(out);
            return out.toByteArray();
        }
    }

    /** Navigates nested maps: {@code path(payload, "data", "request", "body")}. */
    @SuppressWarnings("unchecked")
    public static <T> T path(Map<String, Object> root, String... keys) {
        Object current = root;
        for (String key : keys) {
            current = ((Map<String, Object>) current).get(key);
        }
        return (T) current;
    }
}
