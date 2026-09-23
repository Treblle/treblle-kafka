package com.treblle.kafka;

import com.treblle.kafka.core.Capture;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Reports consumed messages to Treblle, together with how long the application took to handle each
 * one and anything it threw.
 *
 * <p>Kafka has no response, so the SDK asks for the one thing only the application knows: the
 * outcome of processing. Wrapping the handler is what turns {@code response.load_time} into real
 * processing time and populates the {@code errors} array:
 *
 * <pre>{@code
 * TreblleConsumerTracker tracker = treblle.consumerTracker(props);
 *
 * for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
 *     tracker.track(record, r -> orderService.handle(r.value()));
 * }
 * }</pre>
 *
 * <p>Whatever the handler throws propagates unchanged, checked exceptions included. Treblle
 * observes; it never swallows, wraps or alters the application's error handling.
 */
public final class TreblleConsumerTracker {

    /** A consume handler that returns nothing. May throw anything. */
    @FunctionalInterface
    public interface Handler<K, V> {
        void handle(ConsumerRecord<K, V> record) throws Exception;
    }

    /** A consume handler whose return value becomes {@code response.body} in Treblle. */
    @FunctionalInterface
    public interface ReturningHandler<K, V, R> {
        R handle(ConsumerRecord<K, V> record) throws Exception;
    }

    private final Treblle treblle;
    private final ClientInfo clientInfo;

    TreblleConsumerTracker(Treblle treblle, ClientInfo clientInfo) {
        this.treblle = treblle;
        this.clientInfo = clientInfo;
    }

    /** Runs {@code handler} for {@code record} and reports the outcome to Treblle. */
    public <K, V> void track(ConsumerRecord<K, V> record, Handler<K, V> handler) {
        run(record, r -> {
            handler.handle(r);
            return null;
        }, false);
    }

    /**
     * Runs {@code handler} for {@code record}, reports the outcome to Treblle, and returns the
     * handler's value. The returned value is also captured as the payload's {@code response.body}.
     */
    public <K, V, R> R trackResult(ConsumerRecord<K, V> record, ReturningHandler<K, V, R> handler) {
        return run(record, handler, true);
    }

    private <K, V, R> R run(
            ConsumerRecord<K, V> record, ReturningHandler<K, V, R> handler, boolean captureResult) {
        Capture capture = capture(record);
        long startNanos = System.nanoTime();
        try {
            R result = handler.handle(record);
            if (capture != null) {
                // A void handler leaves response.body as {} rather than null - null would read as
                // "the handler returned nothing" when it in fact returns nothing by design.
                capture.hasHandlerResult = captureResult;
                capture.handlerResult = result;
                finish(capture, startNanos, null);
            }
            return result;
        } catch (Throwable t) {
            if (capture != null) {
                finish(capture, startNanos, t);
            }
            // Observe, never interfere: the application sees exactly the exception it would have
            // seen without Treblle in the way.
            throw rethrow(t);
        }
    }

    private <K, V> Capture capture(ConsumerRecord<K, V> record) {
        try {
            if (record == null || !treblle.isTracking(record.topic())) {
                return null;
            }
            Capture capture = new Capture();
            capture.operation = Capture.OPERATION_CONSUME;
            capture.topic = record.topic();
            capture.key = ClientInfo.renderKey(record.key());
            capture.value = record.value();
            capture.headers = ClientInfo.flattenHeaders(record.headers());
            capture.recordTimestampMillis = record.timestamp();
            capture.partition = record.partition();
            capture.offset = record.offset();
            capture.serializedKeySize = record.serializedKeySize();
            capture.serializedValueSize = record.serializedValueSize();
            capture.bootstrapServer = clientInfo.bootstrapServer;
            capture.clientId = clientInfo.clientId;
            capture.groupId = clientInfo.groupId;
            capture.clusterName = clientInfo.clusterName;
            if (record.timestampType() != null) {
                capture.timestampType = record.timestampType().name;
            }
            // Measured now rather than when the payload is built, so worker scheduling cannot
            // inflate the reported lag.
            if (record.timestamp() > 0) {
                capture.consumerLagMillis =
                        Math.max(0, System.currentTimeMillis() - record.timestamp());
            }
            return capture;
        } catch (Throwable t) {
            treblle.logger().error("failed to capture consumed record", t);
            return null;
        }
    }

    private void finish(Capture capture, long startNanos, Throwable failure) {
        try {
            capture.durationMillis = (System.nanoTime() - startNanos) / 1_000_000.0;
            capture.failure = failure;
            capture.metadata = TreblleContext.takeSnapshot();
            treblle.submit(capture);
        } catch (Throwable t) {
            treblle.logger().error("failed to report consumed record", t);
        }
    }

    /**
     * Rethrows {@code t} exactly as thrown, checked or not, without forcing callers to declare
     * {@code throws Exception} on every consume loop.
     */
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException rethrow(Throwable t) throws T {
        throw (T) t;
    }
}
