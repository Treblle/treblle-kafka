package com.treblle.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The handful of facts about a Kafka client that end up in every payload.
 *
 * <p>Read once when a producer is wrapped or a consumer tracker is created, so nothing has to be
 * looked up on the hot path.
 */
final class ClientInfo {

    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String CLIENT_ID = "client.id";
    private static final String GROUP_ID = "group.id";

    /** Truncation limit for a record key rendered into metadata. */
    private static final int MAX_KEY_LENGTH = 128;

    final String bootstrapServer;
    final String clientId;
    final String groupId;
    final String clusterName;

    private ClientInfo(String bootstrapServer, String clientId, String groupId, String clusterName) {
        this.bootstrapServer = bootstrapServer;
        this.clientId = clientId;
        this.groupId = groupId;
        this.clusterName = clusterName;
    }

    /**
     * Reads the relevant keys out of a Kafka client's configuration.
     *
     * @param properties the same {@code Properties} or {@code Map} used to build the client; may
     *     be null
     * @param configuredClusterName the {@code clusterName} option, or null to derive one
     */
    static ClientInfo from(Map<?, ?> properties, String configuredClusterName) {
        String bootstrap = firstBootstrapServer(read(properties, BOOTSTRAP_SERVERS));
        String clientId = asString(read(properties, CLIENT_ID));
        String groupId = asString(read(properties, GROUP_ID));
        String clusterName = configuredClusterName != null && !configuredClusterName.isEmpty()
                ? configuredClusterName
                : hostOf(bootstrap);
        return new ClientInfo(bootstrap, clientId, groupId, clusterName);
    }

    private static Object read(Map<?, ?> properties, String key) {
        if (properties == null) {
            return null;
        }
        try {
            return properties.get(key);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * {@code bootstrap.servers} may be a comma-separated string or a list. Only the first entry is
     * used - it identifies the cluster well enough for the payload's URL and internal id.
     */
    private static String firstBootstrapServer(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Collection) {
            for (Object entry : (Collection<?>) value) {
                String candidate = asString(entry);
                if (candidate != null) {
                    return candidate;
                }
            }
            return null;
        }
        String text = asString(value);
        if (text == null) {
            return null;
        }
        int comma = text.indexOf(',');
        return comma < 0 ? text : text.substring(0, comma).trim();
    }

    private static String hostOf(String bootstrapServer) {
        if (bootstrapServer == null) {
            return null;
        }
        int colon = bootstrapServer.lastIndexOf(':');
        return colon > 0 ? bootstrapServer.substring(0, colon) : bootstrapServer;
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        return text.isEmpty() ? null : text;
    }

    /**
     * Flattens Kafka headers into the string-to-string map Treblle expects. Header values are
     * bytes; anything that is not UTF-8 text still renders, just not meaningfully.
     *
     * <p>Kafka allows repeated header keys. Consistent with the HTTP SDKs, the last value wins.
     */
    static Map<String, String> flattenHeaders(Headers headers) {
        if (headers == null) {
            return java.util.Collections.emptyMap();
        }
        Map<String, String> flattened = new LinkedHashMap<>();
        try {
            for (Header header : headers) {
                if (header == null || header.key() == null) {
                    continue;
                }
                byte[] value = header.value();
                flattened.put(header.key(),
                        value == null ? "" : new String(value, StandardCharsets.UTF_8));
            }
        } catch (RuntimeException e) {
            // A misbehaving header implementation must not stop the message being captured.
        }
        return flattened;
    }

    /** Renders a record key for metadata. Keys are identifiers, not payloads - never bodies. */
    static String renderKey(Object key) {
        if (key == null) {
            return null;
        }
        String text;
        if (key instanceof byte[]) {
            text = new String((byte[]) key, StandardCharsets.UTF_8);
        } else {
            text = key.toString();
        }
        if (text.isEmpty()) {
            return null;
        }
        return text.length() > MAX_KEY_LENGTH ? text.substring(0, MAX_KEY_LENGTH) : text;
    }
}
