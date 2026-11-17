package com.treblle.kafka.internal.transport;

import com.treblle.kafka.TreblleConfig;
import com.treblle.kafka.internal.core.Capture;
import com.treblle.kafka.internal.core.Masker;
import com.treblle.kafka.internal.core.PayloadBuilder;
import com.treblle.kafka.internal.core.PayloadValidator;
import com.treblle.kafka.internal.json.JsonWriter;
import com.treblle.kafka.internal.util.DebugLogger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The boundary between the host application's threads and Treblle.
 *
 * <p>{@link #submit(Capture)} is called from a Kafka I/O thread or an application thread, so it
 * does exactly two things: ask the circuit breaker whether to bother, and hand the capture to a
 * bounded queue. Everything expensive - normalising, masking, validating, serialising, GZIP,
 * HTTP - happens on a single daemon worker thread.
 *
 * <p>The queue is bounded twice: by count ({@link #QUEUE_CAPACITY}, dropping the oldest entry when
 * full) and by the estimated size of the record values it references
 * ({@link #MAX_QUEUED_BYTES}, dropping the newest capture when over budget). Together with the cap
 * on in-flight HTTP requests, a Treblle outage or a burst of large messages can never grow the host
 * application's memory beyond a fixed ceiling.
 */
public final class Dispatcher {

    /** Bounded so a stalled network cannot turn into unbounded memory growth. */
    static final int QUEUE_CAPACITY = 1_000;

    /** Upper bound on the record values the queue may keep alive. */
    static final long MAX_QUEUED_BYTES = 32L * 1024 * 1024;

    private final BlockingQueue<Capture> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicLong queuedBytes = new AtomicLong();
    /** Captures accepted but not yet handed to HTTP: queued plus the one being processed. */
    private final AtomicInteger pending = new AtomicInteger();

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
        this.payloadBuilder = new PayloadBuilder(
                config, new Masker(config.getMaskedKeywords()), kafkaClientVersion);
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
            if (!accepting.get() || !ingress.isUsable()) {
                return false;
            }
            // O(1) check. While the breaker is open we drop rather than queue, so a Treblle
            // outage costs the host application essentially nothing.
            if (!breaker.allowSend()) {
                if (logger.isEnabled()) {
                    logger.log("circuit breaker open; dropped payload for topic " + capture.topic);
                }
                return false;
            }
            long bytes = Math.max(0L, capture.estimatedBytes);
            if (queuedBytes.get() + bytes > MAX_QUEUED_BYTES) {
                if (logger.isEnabled()) {
                    logger.warn("dispatch queue over its memory budget; dropped payload for topic "
                            + capture.topic);
                }
                return false;
            }
            pending.incrementAndGet();
            queuedBytes.addAndGet(bytes);
            if (queue.offer(capture)) {
                return true;
            }
            // Full: make room by discarding the oldest, which is also the least interesting.
            Capture discarded = queue.poll();
            if (discarded != null) {
                release(discarded);
                if (logger.isEnabled()) {
                    logger.warn("dispatch queue full; dropped oldest payload for topic "
                            + discarded.topic);
                }
            }
            if (queue.offer(capture)) {
                return true;
            }
            release(capture);
            return false;
        } catch (RuntimeException e) {
            logger.error("failed to queue payload", e);
            return false;
        }
    }

    private void release(Capture capture) {
        queuedBytes.addAndGet(-Math.max(0L, capture.estimatedBytes));
        pending.decrementAndGet();
    }

    private void drain() {
        while (true) {
            Capture capture;
            try {
                capture = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            queuedBytes.addAndGet(-Math.max(0L, capture.estimatedBytes));
            try {
                process(capture);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                // Nothing that happens in here may kill the worker thread.
                logger.error("failed to process payload", t);
            } finally {
                pending.decrementAndGet();
            }
        }
    }

    private void process(Capture capture) throws InterruptedException {
        Map<String, Object> payload = validator.validate(payloadBuilder.build(capture));
        String json = JsonWriter.write(payload);
        if (logger.isEnabled()) {
            logger.log("sending " + capture.operation + " payload for topic " + capture.topic
                    + " (" + json.length() + " bytes uncompressed)");
        }
        ingress.send(json);
    }

    /**
     * Waits until everything accepted so far has been sent and answered, or the timeout expires.
     *
     * @return true if everything was flushed in time
     */
    public boolean flush(long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        try {
            while (pending.get() > 0) {
                if (System.nanoTime() - deadline >= 0) {
                    return false;
                }
                Thread.sleep(5);
            }
            return ingress.awaitIdle(deadline - System.nanoTime());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Stops accepting new captures, sends what is already queued (waiting at most
     * {@code timeout}), then stops the worker threads. Anything still queued after the timeout is
     * discarded.
     */
    public void close(long timeout, TimeUnit unit) {
        if (!accepting.compareAndSet(true, false)) {
            return;
        }
        boolean flushed = flush(timeout, unit);
        worker.shutdownNow();
        httpExecutor.shutdown();
        if (flushed) {
            logger.log("dispatcher stopped; all payloads flushed");
        } else {
            logger.warn("dispatcher stopped before every payload was sent; "
                    + queue.size() + " discarded");
        }
    }

    boolean isClosed() {
        return !accepting.get();
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
