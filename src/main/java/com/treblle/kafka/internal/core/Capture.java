package com.treblle.kafka.internal.core;

import java.util.Collections;
import java.util.Map;

/**
 * A snapshot of one Kafka operation, taken on the hot path.
 *
 * <p>This is deliberately a dumb struct of cheap references. Building it must cost close to
 * nothing: no masking, no JSON, no serialisation. The dispatcher's worker thread turns it into a
 * Treblle payload later, off the caller's thread.
 *
 * <p>Internal SDK type. Not part of the public API.
 */
public final class Capture {

    /** Kafka produce - reported to Treblle as an HTTP {@code POST}. */
    public static final String OPERATION_PRODUCE = "produce";

    /** Kafka consume - reported to Treblle as an HTTP {@code GET}. */
    public static final String OPERATION_CONSUME = "consume";

    /** Either {@link #OPERATION_PRODUCE} or {@link #OPERATION_CONSUME}. */
    public String operation;

    /** Topic name. Becomes {@code request.route_path} - the AsyncAPI channel. */
    public String topic;

    /**
     * Record key rendered as a string, or null when absent or when {@code captureRecordKeys} is
     * off. Reported in metadata, never as the body.
     */
    public String key;

    /**
     * The record value, captured by reference.
     *
     * <p>Deep-copying every message on the caller's thread would defeat the SDK's zero-overhead
     * goal, so the worker takes a private copy as its first step. A value the application mutates
     * in the meantime may be captured in its mutated state; one mutated <em>during</em> the copy is
     * replaced with an explanatory placeholder rather than dropped. Values over the body limit are
     * swapped for a placeholder on the caller's thread, so the queue never holds them.
     */
    public Object value;

    /**
     * Rough size in bytes of what this capture keeps alive (mostly {@link #value}). Computed on
     * the caller's thread from O(1) facts only; drives the dispatcher's memory budget.
     */
    public long estimatedBytes;

    /** Record headers, already flattened to strings. Last value wins for repeated keys. */
    public Map<String, String> headers = Collections.emptyMap();

    /**
     * When the operation happened, in epoch millis: the {@code send()} call, or the moment the
     * consume handler started. Becomes {@code request.timestamp}.
     */
    public long operationTimestampMillis;

    /** The record's own Kafka timestamp in epoch millis, or 0 when unavailable. */
    public long recordTimestampMillis;

    /** Kafka's timestamp type, e.g. {@code CreateTime}. */
    public String timestampType;

    /** First entry of {@code bootstrap.servers}, used to build the {@code kafka://} URL. */
    public String bootstrapServer;

    /** The client's configured {@code client.id}, or null. */
    public String clientId;

    /** The consumer's {@code group.id}. Null on the produce side. */
    public String groupId;

    /** Friendly cluster name for {@code internal_name}. */
    public String clusterName;

    /** Partition the record landed on / came from, or -1 when unknown. */
    public int partition = -1;

    /** Offset the record landed on / came from, or -1 when unknown. */
    public long offset = -1;

    /** Serialised value size in bytes, or -1 when unknown. */
    public int serializedValueSize = -1;

    /** Serialised key size in bytes, or -1 when unknown. */
    public int serializedKeySize = -1;

    /**
     * Time in milliseconds that Treblle reports as {@code response.load_time}.
     *
     * <p>Produce: from the {@code send()} call to the broker acknowledgement. Consume: the wall
     * time of the user's handler.
     */
    public double durationMillis;

    /** Broker acknowledgement timestamp in epoch millis (produce only), or 0. */
    public long ackTimestampMillis;

    /**
     * Milliseconds between the record's timestamp and the moment the consumer picked it up.
     *
     * <p>Measured in the collector, not when the payload is built, so worker-thread scheduling
     * cannot inflate it. -1 when unknown.
     */
    public long consumerLagMillis = -1;

    /** The failure, if the send was rejected or the consume handler threw. Null on success. */
    public Throwable failure;

    /** Value returned by the consume handler, when the returning overload is used. */
    public Object handlerResult;

    /** Whether the consume handler returns a value that should populate {@code response.body}. */
    public boolean hasHandlerResult;

    /** Custom metadata snapshotted from {@link com.treblle.kafka.TreblleContext}. */
    public Map<String, Object> metadata = Collections.emptyMap();

    public boolean isProduce() {
        return OPERATION_PRODUCE.equals(operation);
    }
}
