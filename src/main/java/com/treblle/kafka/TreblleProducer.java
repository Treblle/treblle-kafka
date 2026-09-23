package com.treblle.kafka;

import com.treblle.kafka.core.Capture;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

/**
 * A {@link Producer} that reports everything it sends to Treblle.
 *
 * <p>A drop-in decorator - every method delegates to the real producer, and {@code send} is the
 * only one that does anything extra. Obtain one via {@link Treblle#wrap}:
 *
 * <pre>{@code
 * Producer<String, String> producer = treblle.wrap(new KafkaProducer<>(props), props);
 * }</pre>
 *
 * <p>The decorator sits <em>above</em> serialisation, so it sees the typed value the application
 * passed in rather than the bytes on the wire. That is what makes a JSON message readable in
 * Treblle without a schema registry.
 *
 * <p>Correlating a send with its broker acknowledgement is exact here, because the decorator owns
 * the callback. It also works on every {@code kafka-clients} version, which a
 * {@code ProducerInterceptor} does not.
 */
final class TreblleProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> delegate;
    private final Treblle treblle;
    private final ClientInfo clientInfo;

    TreblleProducer(Producer<K, V> delegate, Treblle treblle, ClientInfo clientInfo) {
        this.delegate = delegate;
        this.treblle = treblle;
        this.clientInfo = clientInfo;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        Capture capture = capture(record);
        if (capture == null) {
            return delegate.send(record, callback);
        }

        long startNanos = System.nanoTime();
        try {
            return delegate.send(record, (metadata, exception) -> {
                // Runs on the producer's I/O thread. Everything here must be cheap and safe:
                // fill in the ack details and hand the capture to the dispatcher, nothing more.
                try {
                    complete(capture, metadata, exception, startNanos);
                } catch (Throwable t) {
                    treblle.logger().error("failed to capture acknowledgement", t);
                }
                if (callback != null) {
                    callback.onCompletion(metadata, exception);
                }
            });
        } catch (Throwable t) {
            // send() can fail synchronously - a serialisation error, or a full buffer with
            // max.block.ms exceeded. The callback never fires, so record the failure here and
            // rethrow untouched: the application must see exactly what it would without Treblle.
            try {
                capture.failure = t;
                capture.durationMillis = elapsedMillis(startNanos);
                treblle.submit(capture);
            } catch (Throwable ignored) {
                // Never let SDK bookkeeping change what the application sees.
            }
            throw t;
        }
    }

    /**
     * Takes the hot-path snapshot, or returns null when this record is not tracked.
     *
     * <p>Cheap by design: references to the key and value, a flattened header map, and the
     * thread's metadata. No masking, no JSON, no serialisation.
     */
    private Capture capture(ProducerRecord<K, V> record) {
        try {
            if (record == null || !treblle.isTracking(record.topic())) {
                return null;
            }
            Capture capture = new Capture();
            capture.operation = Capture.OPERATION_PRODUCE;
            capture.topic = record.topic();
            capture.key = ClientInfo.renderKey(record.key());
            capture.value = record.value();
            capture.headers = ClientInfo.flattenHeaders(record.headers());
            capture.recordTimestampMillis =
                    record.timestamp() != null ? record.timestamp() : System.currentTimeMillis();
            capture.bootstrapServer = clientInfo.bootstrapServer;
            capture.clientId = clientInfo.clientId;
            capture.clusterName = clientInfo.clusterName;
            if (record.partition() != null) {
                capture.partition = record.partition();
            }
            capture.metadata = TreblleContext.takeSnapshot();
            return capture;
        } catch (Throwable t) {
            treblle.logger().error("failed to capture produced record", t);
            return null;
        }
    }

    private void complete(
            Capture capture, RecordMetadata metadata, Exception exception, long startNanos) {
        capture.durationMillis = elapsedMillis(startNanos);
        capture.failure = exception;
        if (metadata != null) {
            capture.partition = metadata.partition();
            if (metadata.hasOffset()) {
                capture.offset = metadata.offset();
            }
            if (metadata.hasTimestamp()) {
                capture.ackTimestampMillis = metadata.timestamp();
            }
            capture.serializedKeySize = metadata.serializedKeySize();
            capture.serializedValueSize = metadata.serializedValueSize();
        }
        treblle.submit(capture);
    }

    private static double elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }

    // --- Everything below is pure delegation. -------------------------------------------------

    @Override
    public void initTransactions() {
        delegate.initTransactions();
    }

    @Override
    public void beginTransaction() {
        delegate.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(
            Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
        delegate.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() {
        delegate.commitTransaction();
    }

    @Override
    public void abortTransaction() {
        delegate.abortTransaction();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }
}
