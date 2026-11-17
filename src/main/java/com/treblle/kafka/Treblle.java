package com.treblle.kafka;

import com.treblle.kafka.internal.core.Capture;
import com.treblle.kafka.internal.transport.Dispatcher;
import com.treblle.kafka.internal.util.DebugLogger;
import com.treblle.kafka.internal.util.TopicFilter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;

/**
 * The Treblle Kafka SDK.
 *
 * <p>Build one instance for the lifetime of your application, then use it to wrap producers and to
 * track consumed messages:
 *
 * <pre>{@code
 * Treblle treblle = Treblle.builder()
 *         .sdkToken(System.getenv("TREBLLE_SDK_TOKEN"))
 *         .apiKey(System.getenv("TREBLLE_API_KEY"))
 *         .maskedKeywords("password", "authorization")
 *         .build();
 *
 * Producer<String, String> producer = treblle.wrap(new KafkaProducer<>(props), props);
 * TreblleConsumerTracker tracker = treblle.consumerTracker(consumerProps);
 * }</pre>
 *
 * <p>Call {@link #close()} when your application shuts down; it sends anything still queued. As a
 * safety net the SDK also registers a JVM shutdown hook that does the same if you forget.
 *
 * <p>If the SDK is disabled or misconfigured, {@link #wrap} hands back the producer untouched and
 * {@link TreblleConsumerTracker} becomes a pass-through, so there is no overhead at all - not even
 * a branch per message beyond the wrapper itself.
 */
public final class Treblle implements AutoCloseable {

    private final TreblleConfig config;
    private final DebugLogger logger;
    private final TopicFilter topicFilter;
    /** How long {@link #close()} and the shutdown hook wait for queued payloads to be sent. */
    public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final Dispatcher dispatcher;
    private final Thread shutdownHook;
    private volatile boolean closed;

    private Treblle(TreblleConfig config) {
        this.config = config;
        this.logger = new DebugLogger(config.isDebug());
        this.topicFilter = new TopicFilter(config.getIncludedTopics(), config.getExcludedTopics());

        logger.log("initialising Treblle Kafka SDK: " + config.describe());

        if (!config.isEnabled()) {
            logger.warn("SDK is disabled (enabled=false); no data will be sent");
            this.dispatcher = null;
            this.shutdownHook = null;
            return;
        }
        if (!config.isValid()) {
            logger.warn("sdkToken and apiKey are both required; SDK disabled. "
                    + "Set them on the builder, or via TREBLLE_SDK_TOKEN / TREBLLE_API_KEY.");
            this.dispatcher = null;
            this.shutdownHook = null;
            return;
        }

        this.dispatcher = new Dispatcher(config, logger, kafkaClientVersion());
        this.shutdownHook = registerShutdownHook();
        logger.log("Treblle Kafka SDK is active");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builds an SDK instance from an already-resolved configuration. */
    public static Treblle create(TreblleConfig config) {
        return new Treblle(config);
    }

    /**
     * Wraps a Kafka producer so every message it sends is reported to Treblle.
     *
     * @param producer the producer to wrap
     * @param producerProperties the properties the producer was built with, read once for
     *     {@code bootstrap.servers} and {@code client.id}
     * @return a decorated producer, or {@code producer} unchanged when the SDK is inactive
     */
    public <K, V> Producer<K, V> wrap(Producer<K, V> producer, Properties producerProperties) {
        return wrapInternal(producer, producerProperties);
    }

    /** Wraps a Kafka producer configured from a {@code Map}. */
    public <K, V> Producer<K, V> wrap(Producer<K, V> producer, Map<String, ?> producerProperties) {
        return wrapInternal(producer, producerProperties);
    }

    /** Wraps a Kafka producer whose configuration is not available. */
    public <K, V> Producer<K, V> wrap(Producer<K, V> producer) {
        return wrapInternal(producer, null);
    }

    private <K, V> Producer<K, V> wrapInternal(Producer<K, V> producer, Map<?, ?> properties) {
        if (producer == null) {
            return null;
        }
        if (!isActive()) {
            logger.log("SDK inactive; returning the producer unwrapped");
            return producer;
        }
        return new TreblleProducer<>(producer, this, clientInfo(properties));
    }

    /**
     * Creates a tracker for a Kafka consumer.
     *
     * @param consumerProperties the properties the consumer was built with, read once for
     *     {@code bootstrap.servers}, {@code client.id} and {@code group.id}
     */
    public TreblleConsumerTracker consumerTracker(Properties consumerProperties) {
        return new TreblleConsumerTracker(this, clientInfo(consumerProperties));
    }

    /** Creates a tracker for a Kafka consumer configured from a {@code Map}. */
    public TreblleConsumerTracker consumerTracker(Map<String, ?> consumerProperties) {
        return new TreblleConsumerTracker(this, clientInfo(consumerProperties));
    }

    /** Creates a tracker for a consumer whose configuration is not available. */
    public TreblleConsumerTracker consumerTracker() {
        return new TreblleConsumerTracker(this, clientInfo(null));
    }

    /** True when the SDK is enabled, correctly configured and not closed. */
    public boolean isActive() {
        return dispatcher != null && !closed;
    }

    public TreblleConfig getConfig() {
        return config;
    }

    /**
     * Waits until every message captured so far has been sent to Treblle, or the timeout expires.
     * Useful at the end of a batch job, or in tests. Never throws.
     *
     * @return true if everything was sent in time
     */
    public boolean flush(Duration timeout) {
        if (dispatcher == null || timeout == null) {
            return true;
        }
        return dispatcher.flush(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Sends anything still queued (waiting up to {@link #DEFAULT_CLOSE_TIMEOUT}), then stops the
     * background threads. Wrapped producers keep working; they just stop reporting.
     */
    @Override
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT);
    }

    /**
     * Sends anything still queued (waiting up to {@code timeout}), then stops the background
     * threads. Payloads still queued after the timeout are discarded. Idempotent.
     */
    public void close(Duration timeout) {
        if (closed) {
            return;
        }
        closed = true;
        if (dispatcher == null) {
            return;
        }
        removeShutdownHook();
        Duration wait = timeout == null ? DEFAULT_CLOSE_TIMEOUT : timeout;
        dispatcher.close(wait.toNanos(), TimeUnit.NANOSECONDS);
    }

    private Thread registerShutdownHook() {
        Thread hook = new Thread(() -> {
            if (!closed) {
                closed = true;
                dispatcher.close(DEFAULT_CLOSE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
            }
        }, "treblle-kafka-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            return hook;
        } catch (RuntimeException e) {
            // Already shutting down, or a SecurityManager forbids hooks. Explicit close() still works.
            logger.error("could not register shutdown hook", e);
            return null;
        }
    }

    private void removeShutdownHook() {
        if (shutdownHook == null || Thread.currentThread() == shutdownHook) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (RuntimeException ignored) {
            // IllegalStateException: the JVM is already shutting down and the hook will run.
        }
    }

    // --- Internal API used by the collectors. -------------------------------------------------

    DebugLogger logger() {
        return logger;
    }

    boolean isTracking(String topic) {
        if (dispatcher == null || closed) {
            return false;
        }
        if (topicFilter.isExcluded(topic)) {
            if (logger.isEnabled()) {
                logger.log("topic '" + topic + "' is excluded; skipping");
            }
            return false;
        }
        return true;
    }

    void submit(Capture capture) {
        if (dispatcher != null) {
            dispatcher.submit(capture);
        }
    }

    private ClientInfo clientInfo(Map<?, ?> properties) {
        return ClientInfo.from(properties, config.getClusterName());
    }

    /** The kafka-clients version on the classpath, reported as {@code server.software}. */
    private static String kafkaClientVersion() {
        try {
            return org.apache.kafka.common.utils.AppInfoParser.getVersion();
        } catch (Throwable t) {
            return "unknown";
        }
    }

    /**
     * Fluent builder for the SDK. Every option falls back to a {@code treblle.*} system property,
     * then a {@code TREBLLE_*} environment variable, then its default.
     */
    public static final class Builder {

        private final TreblleConfig.Builder config = TreblleConfig.builder();

        private Builder() {
        }

        /** SDK Token from the Treblle Dashboard. Required. */
        public Builder sdkToken(String sdkToken) {
            config.sdkToken(sdkToken);
            return this;
        }

        /** API Key from the Treblle Dashboard. Required. */
        public Builder apiKey(String apiKey) {
            config.apiKey(apiKey);
            return this;
        }

        /** Enables local logging of all SDK activity. Defaults to false. */
        public Builder debug(boolean debug) {
            config.debug(debug);
            return this;
        }

        /** Master switch. When false the SDK sends nothing. Defaults to true. */
        public Builder enabled(boolean enabled) {
            config.enabled(enabled);
            return this;
        }

        /** Ingestion URL. Defaults to {@code https://ingress.treblle.com}. */
        public Builder ingressEndpoint(String ingressEndpoint) {
            config.ingressEndpoint(ingressEndpoint);
            return this;
        }

        /** Friendly name for this Kafka cluster. Defaults to the first bootstrap server host. */
        public Builder clusterName(String clusterName) {
            config.clusterName(clusterName);
            return this;
        }

        /**
         * Extra keys whose values are masked before sending, on top of
         * {@link TreblleConfig#DEFAULT_MASKED_KEYWORDS}. The defaults always apply.
         */
        public Builder maskedKeywords(String... maskedKeywords) {
            config.maskedKeywords(maskedKeywords);
            return this;
        }

        /** Extra keys whose values are masked. See the varargs overload. */
        public Builder maskedKeywords(List<String> maskedKeywords) {
            config.maskedKeywords(maskedKeywords);
            return this;
        }

        /**
         * Topics the SDK must track, to the exclusion of all others. Supports exact names and
         * {@code prefix.*} wildcards. Empty (the default) means every topic is a candidate. See
         * {@link #excludedTopics(String...)} for how the two combine.
         */
        public Builder includedTopics(String... includedTopics) {
            config.includedTopics(includedTopics);
            return this;
        }

        /** Topics the SDK must track, to the exclusion of all others. See the varargs overload. */
        public Builder includedTopics(List<String> includedTopics) {
            config.includedTopics(includedTopics);
            return this;
        }

        /**
         * Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards.
         * Applied after {@code includedTopics}, and wins when a topic matches both.
         */
        public Builder excludedTopics(String... excludedTopics) {
            config.excludedTopics(excludedTopics);
            return this;
        }

        /** Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards. */
        public Builder excludedTopics(List<String> excludedTopics) {
            config.excludedTopics(excludedTopics);
            return this;
        }

        /**
         * Whether record keys are reported as {@code kafka.key} metadata. Defaults to true. Turn it
         * off if your keys carry personal data such as email addresses.
         */
        public Builder captureRecordKeys(boolean captureRecordKeys) {
            config.captureRecordKeys(captureRecordKeys);
            return this;
        }

        /**
         * Renders record values the SDK does not understand natively (typically POJOs) into JSON,
         * e.g. {@code objectMapper::writeValueAsString}. See {@link ValueRenderer}.
         */
        public Builder valueRenderer(ValueRenderer valueRenderer) {
            config.valueRenderer(valueRenderer);
            return this;
        }

        public Treblle build() {
            return new Treblle(config.build());
        }
    }
}
