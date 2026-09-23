package com.treblle.kafka;

import com.treblle.kafka.core.Capture;
import com.treblle.kafka.transport.Dispatcher;
import com.treblle.kafka.util.DebugLogger;
import com.treblle.kafka.util.TopicFilter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

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
 * <p>If the SDK is disabled or misconfigured, {@link #wrap} hands back the producer untouched and
 * {@link TreblleConsumerTracker} becomes a pass-through, so there is no overhead at all - not even
 * a branch per message beyond the wrapper itself.
 */
public final class Treblle implements AutoCloseable {

    private final TreblleConfig config;
    private final DebugLogger logger;
    private final TopicFilter topicFilter;
    private final Dispatcher dispatcher;

    private Treblle(TreblleConfig config) {
        this.config = config;
        this.logger = new DebugLogger(config.isDebug());
        this.topicFilter = new TopicFilter(config.getExcludedTopics());

        logger.log("initialising Treblle Kafka SDK: " + config.describe());

        if (!config.isEnabled()) {
            logger.warn("SDK is disabled (enabled=false); no data will be sent");
            this.dispatcher = null;
            return;
        }
        if (!config.isValid()) {
            logger.warn("sdkToken and apiKey are both required; SDK disabled. "
                    + "Set them on the builder, or via TREBLLE_SDK_TOKEN / TREBLLE_API_KEY.");
            this.dispatcher = null;
            return;
        }
        if (config.getMaskedKeywords().isEmpty()) {
            logger.warn("maskedKeywords is empty; masking is disabled and message bodies and "
                    + "headers will be sent to Treblle as-is");
        }

        this.dispatcher = new Dispatcher(config, logger, kafkaClientVersion());
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

    /** True when the SDK is enabled and correctly configured. */
    public boolean isActive() {
        return dispatcher != null;
    }

    public TreblleConfig getConfig() {
        return config;
    }

    /** Stops the background worker threads. Wrapped producers keep working; they just stop reporting. */
    @Override
    public void close() {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    // --- Internal API used by the collectors. -------------------------------------------------

    DebugLogger logger() {
        return logger;
    }

    boolean isTracking(String topic) {
        if (dispatcher == null) {
            return false;
        }
        if (topicFilter.isExcluded(topic)) {
            logger.log("topic '" + topic + "' is excluded; skipping");
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

        /** Keys whose values are masked before sending. Empty disables masking entirely. */
        public Builder maskedKeywords(String... maskedKeywords) {
            config.maskedKeywords(maskedKeywords);
            return this;
        }

        /** Keys whose values are masked before sending. Empty disables masking entirely. */
        public Builder maskedKeywords(List<String> maskedKeywords) {
            config.maskedKeywords(maskedKeywords);
            return this;
        }

        /** Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards. */
        public Builder excludedTopics(String... excludedTopics) {
            config.excludedTopics(excludedTopics);
            return this;
        }

        /** Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards. */
        public Builder excludedTopics(List<String> excludedTopics) {
            config.excludedTopics(excludedTopics);
            return this;
        }

        public Treblle build() {
            return new Treblle(config.build());
        }
    }
}
