package com.treblle.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Resolved SDK configuration.
 *
 * <p>Every option can be set three ways, in descending priority:
 *
 * <ol>
 *   <li>explicitly on the {@link Builder}
 *   <li>a JVM system property, e.g. {@code -Dtreblle.sdk.token=...}
 *   <li>an environment variable, e.g. {@code TREBLLE_SDK_TOKEN}
 * </ol>
 *
 * <p>The SDK never throws over bad configuration. If {@code sdkToken} or {@code apiKey} is
 * missing, {@link #isValid()} returns false, the SDK disables itself, and a warning is emitted in
 * debug mode.
 */
public final class TreblleConfig {

    public static final String DEFAULT_INGRESS_ENDPOINT = "https://ingress.treblle.com";

    private final String sdkToken;
    private final String apiKey;
    private final boolean debug;
    private final boolean enabled;
    private final String ingressEndpoint;
    private final String clusterName;
    private final Set<String> maskedKeywords;
    private final List<String> excludedTopics;

    private TreblleConfig(Builder builder) {
        this.sdkToken = resolve(builder.sdkToken, "treblle.sdk.token", "TREBLLE_SDK_TOKEN", null);
        this.apiKey = resolve(builder.apiKey, "treblle.api.key", "TREBLLE_API_KEY", null);
        this.debug = resolveBoolean(builder.debug, "treblle.debug", "TREBLLE_DEBUG", false);
        this.enabled = resolveBoolean(builder.enabled, "treblle.enabled", "TREBLLE_ENABLED", true);
        this.ingressEndpoint = resolve(
                builder.ingressEndpoint,
                "treblle.ingress.endpoint",
                "TREBLLE_INGRESS_ENDPOINT",
                DEFAULT_INGRESS_ENDPOINT);
        this.clusterName =
                resolve(builder.clusterName, "treblle.cluster.name", "TREBLLE_CLUSTER_NAME", null);
        this.maskedKeywords = Collections.unmodifiableSet(lowercased(resolveList(
                builder.maskedKeywords, "treblle.masked.keywords", "TREBLLE_MASKED_KEYWORDS")));
        this.excludedTopics = Collections.unmodifiableList(resolveList(
                builder.excludedTopics, "treblle.excluded.topics", "TREBLLE_EXCLUDED_TOPICS"));
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSdkToken() {
        return sdkToken;
    }

    public String getApiKey() {
        return apiKey;
    }

    public boolean isDebug() {
        return debug;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getIngressEndpoint() {
        return ingressEndpoint;
    }

    public String getClusterName() {
        return clusterName;
    }

    /** Keywords to mask, already lowercased for case-insensitive comparison. */
    public Set<String> getMaskedKeywords() {
        return maskedKeywords;
    }

    public List<String> getExcludedTopics() {
        return excludedTopics;
    }

    /**
     * True when the SDK has everything it needs to send data. False disables the SDK entirely -
     * wrapped producers are returned unwrapped and no payload is ever built.
     */
    public boolean isValid() {
        return enabled && isNotBlank(sdkToken) && isNotBlank(apiKey);
    }

    /** A human-readable summary for debug mode. The SDK token is never logged in full. */
    public String describe() {
        return "enabled=" + enabled
                + ", valid=" + isValid()
                + ", sdkToken=" + redact(sdkToken)
                + ", apiKey=" + redact(apiKey)
                + ", ingressEndpoint=" + ingressEndpoint
                + ", maskedKeywords=" + maskedKeywords.size()
                + ", excludedTopics=" + excludedTopics;
    }

    private static String redact(String value) {
        if (!isNotBlank(value)) {
            return "<missing>";
        }
        if (value.length() <= 4) {
            return "****";
        }
        return value.substring(0, 4) + "****";
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String resolve(
            String explicit, String systemProperty, String environmentVariable, String fallback) {
        if (isNotBlank(explicit)) {
            return explicit.trim();
        }
        String property = readSystemProperty(systemProperty);
        if (isNotBlank(property)) {
            return property.trim();
        }
        String environment = readEnvironment(environmentVariable);
        if (isNotBlank(environment)) {
            return environment.trim();
        }
        return fallback;
    }

    private static boolean resolveBoolean(
            Boolean explicit, String systemProperty, String environmentVariable, boolean fallback) {
        if (explicit != null) {
            return explicit;
        }
        String raw = resolve(null, systemProperty, environmentVariable, null);
        if (raw == null) {
            return fallback;
        }
        String normalised = raw.toLowerCase(Locale.ROOT);
        return normalised.equals("true") || normalised.equals("1") || normalised.equals("yes");
    }

    private static List<String> resolveList(
            List<String> explicit, String systemProperty, String environmentVariable) {
        if (explicit != null && !explicit.isEmpty()) {
            return new ArrayList<>(explicit);
        }
        String raw = resolve(null, systemProperty, environmentVariable, null);
        List<String> values = new ArrayList<>();
        if (raw == null) {
            return values;
        }
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }

    private static Set<String> lowercased(List<String> values) {
        Set<String> result = new HashSet<>();
        for (String value : values) {
            result.add(value.toLowerCase(Locale.ROOT));
        }
        return result;
    }

    private static String readSystemProperty(String key) {
        try {
            return System.getProperty(key);
        } catch (SecurityException e) {
            // A restrictive SecurityManager must not break the host application.
            return null;
        }
    }

    private static String readEnvironment(String key) {
        try {
            return System.getenv(key);
        } catch (SecurityException e) {
            return null;
        }
    }

    /** Fluent builder. All methods are optional; unset options fall back to system property, env, default. */
    public static final class Builder {

        private String sdkToken;
        private String apiKey;
        private Boolean debug;
        private Boolean enabled;
        private String ingressEndpoint;
        private String clusterName;
        private List<String> maskedKeywords;
        private List<String> excludedTopics;

        private Builder() {
        }

        /** SDK Token from the Treblle Dashboard. Required. */
        public Builder sdkToken(String sdkToken) {
            this.sdkToken = sdkToken;
            return this;
        }

        /** API Key from the Treblle Dashboard. Required. */
        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /** Enables local logging of all SDK activity. Defaults to false. */
        public Builder debug(boolean debug) {
            this.debug = debug;
            return this;
        }

        /** Master switch. When false the SDK sends nothing. Defaults to true. */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /** Ingestion URL. Defaults to {@value TreblleConfig#DEFAULT_INGRESS_ENDPOINT}. */
        public Builder ingressEndpoint(String ingressEndpoint) {
            this.ingressEndpoint = ingressEndpoint;
            return this;
        }

        /**
         * Friendly name for this Kafka cluster. Defaults to the first bootstrap server host. Folded
         * into every payload's {@code internal_id} alongside the topic name, so topics on this
         * cluster auto-discover as their own APIs without colliding with same-named topics on
         * another cluster sharing the same credentials.
         */
        public Builder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        /** Keys whose values are masked before sending. Empty disables masking entirely. */
        public Builder maskedKeywords(String... maskedKeywords) {
            this.maskedKeywords = Arrays.asList(maskedKeywords);
            return this;
        }

        /** Keys whose values are masked before sending. Empty disables masking entirely. */
        public Builder maskedKeywords(List<String> maskedKeywords) {
            this.maskedKeywords = maskedKeywords;
            return this;
        }

        /** Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards. */
        public Builder excludedTopics(String... excludedTopics) {
            this.excludedTopics = Arrays.asList(excludedTopics);
            return this;
        }

        /** Topics the SDK must not track. Supports exact names and {@code prefix.*} wildcards. */
        public Builder excludedTopics(List<String> excludedTopics) {
            this.excludedTopics = excludedTopics;
            return this;
        }

        public TreblleConfig build() {
            return new TreblleConfig(this);
        }
    }
}
