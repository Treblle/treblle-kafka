package com.treblle.kafka.transport;

import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.core.Capture;
import com.treblle.kafka.core.Masker;
import com.treblle.kafka.core.PayloadBuilder;
import com.treblle.kafka.core.PayloadValidator;
import com.treblle.kafka.json.JsonWriter;
import com.treblle.kafka.util.DebugLogger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The boundary between the host application's threads and Treblle.
 *
 * <p>{@link #submit(Capture)} is called from a Kafka I/O thread or an application thread, so it
 * does exactly two things: ask the circuit breaker whether to bother, and hand the capture to a
 * bounded queue. Everything expensive - normalising, masking, validating, serialising, GZIP,
 * HTTP - happens on a single daemon worker thread.
 *
 * <p>The queue is bounded and drops its oldest entry when full. A Treblle outage or a burst of
 * traffic can therefore never grow the host application's memory.
 */
public final class Dispatcher {

    /** Bounded so a stalled network cannot turn into unbounded memory growth. */
    private static final int QUEUE_CAPACITY = 1_000;

    private final BlockingQueue<Capture> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final DebugLogger logger;
    private final CircuitBreaker breaker;
    private final PayloadBuilder payloadBuilder;
    private final PayloadValidator validator;
    private final IngressClient ingress;
    private final ExecutorService worker;
    private final ExecutorService httpExecutor;

    public Dispatcher(TreblleConfig config, DebugLogger logger, String kafkaClientVersion) {
        this.logger = logger;
        this.breaker = new CircuitBreaker(logger);
        this.payloadBuilder =
                new PayloadBuilder(config, new Masker(config.getMaskedKeywords()), kafkaClientVersion);
        this.validator = new PayloadValidator(logger);
        this.httpExecutor = Executors.newFixedThreadPool(2, daemonThreads("treblle-kafka-http"));
        this.ingress = new IngressClient(config, logger, breaker, (Executor) httpExecutor);
        this.worker = Executors.newSingleThreadExecutor(daemonThreads("treblle-kafka-dispatcher"));
        this.worker.submit(this::drain);
    }

    /**
     * Hands a capture off for sending. Never blocks, never throws.
     *
     * @return true if the capture was accepted for sending
     */
    public boolean submit(Capture capture) {
        try {
            if (!running.get()) {
                return false;
            }
            if (!ingress.isUsable()) {
                return false;
            }
            // O(1) check. While the breaker is open we drop rather than queue, so a Treblle
            // outage costs the host application essentially nothing.
            if (!breaker.allowSend()) {
                logger.log("circuit breaker open; dropped payload for topic " + capture.topic);
                return false;
            }
            if (!queue.offer(capture)) {
                // Full: make room by discarding the oldest, which is also the least interesting.
                Capture discarded = queue.poll();
                if (discarded != null) {
                    logger.warn("dispatch queue full; dropped oldest payload for topic "
                            + discarded.topic);
                }
                return queue.offer(capture);
            }
            return true;
        } catch (RuntimeException e) {
            logger.error("failed to queue payload", e);
            return false;
        }
    }

    private void drain() {
        while (running.get()) {
            Capture capture;
            try {
                capture = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                process(capture);
            } catch (Throwable t) {
                // Nothing that happens in here may kill the worker thread.
                logger.error("failed to process payload", t);
            }
        }
    }

    private void process(Capture capture) {
        Map<String, Object> payload = validator.validate(payloadBuilder.build(capture));
        String json = JsonWriter.write(payload);
        logger.log("sending " + capture.operation + " payload for topic " + capture.topic
                + " (" + json.length() + " bytes uncompressed)");
        ingress.send(json);
    }

    /** Stops the worker threads. Queued payloads are discarded. */
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        worker.shutdownNow();
        httpExecutor.shutdown();
        logger.log("dispatcher stopped");
    }

    private static ThreadFactory daemonThreads(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            // Daemon: the SDK must never keep a shutting-down application alive.
            thread.setDaemon(true);
            thread.setPriority(Thread.MIN_PRIORITY);
            return thread;
        };
    }
}
