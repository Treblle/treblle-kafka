package com.treblle.kafka.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides which topics the SDK tracks.
 *
 * <p>This is the Kafka equivalent of the HTTP SDKs' {@code excludedPaths}. It supports exact topic
 * names ({@code payments.audit}) and trailing wildcards ({@code internal.*}, which excludes every
 * topic starting with {@code internal.}). Matching is case-sensitive, matching the rest of the
 * Treblle SDK family.
 *
 * <p>Kafka's own internal topics are always excluded regardless of configuration - they carry no
 * application traffic and would only add noise. This mirrors how the HTTP SDKs automatically skip
 * static assets and {@code .well-known} paths.
 */
public final class TopicFilter {

    private static final Set<String> ALWAYS_EXCLUDED = new HashSet<>(java.util.Arrays.asList(
            "_schemas",
            "_confluent-license",
            "_confluent-command"));

    private final Set<String> exactMatches = new HashSet<>();
    private final List<String> prefixMatches = new ArrayList<>();

    public TopicFilter(Collection<String> excludedTopics) {
        if (excludedTopics == null) {
            return;
        }
        for (String entry : excludedTopics) {
            if (entry == null) {
                continue;
            }
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.endsWith("*")) {
                prefixMatches.add(trimmed.substring(0, trimmed.length() - 1));
            } else {
                exactMatches.add(trimmed);
            }
        }
    }

    /** Returns true when the topic must not be tracked. */
    public boolean isExcluded(String topic) {
        if (topic == null || topic.isEmpty()) {
            return true;
        }
        // Kafka's internal bookkeeping topics: __consumer_offsets, __transaction_state, etc.
        if (topic.startsWith("__") || ALWAYS_EXCLUDED.contains(topic)) {
            return true;
        }
        if (exactMatches.contains(topic)) {
            return true;
        }
        for (String prefix : prefixMatches) {
            if (topic.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
