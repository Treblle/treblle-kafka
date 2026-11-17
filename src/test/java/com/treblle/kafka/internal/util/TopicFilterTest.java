package com.treblle.kafka.internal.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class TopicFilterTest {

    @Test
    void tracksEverythingByDefaultExceptKafkaInternals() {
        TopicFilter filter = new TopicFilter(List.of(), List.of());
        assertFalse(filter.isExcluded("orders"));
        assertTrue(filter.isExcluded("__consumer_offsets"));
        assertTrue(filter.isExcluded("_schemas"));
        assertTrue(filter.isExcluded(null));
        assertTrue(filter.isExcluded(""));
    }

    @Test
    void includedTopicsActAsAllowList() {
        TopicFilter filter = new TopicFilter(List.of("orders.*", "payments"), List.of());
        assertFalse(filter.isExcluded("orders.created"));
        assertFalse(filter.isExcluded("payments"));
        assertTrue(filter.isExcluded("payments.audit"));
        assertTrue(filter.isExcluded("shipping"));
    }

    @Test
    void excludedTopicsWinOverIncluded() {
        TopicFilter filter = new TopicFilter(List.of("payments.*"), List.of("payments.audit"));
        assertFalse(filter.isExcluded("payments.created"));
        assertTrue(filter.isExcluded("payments.audit"));
    }

    @Test
    void matchingIsCaseSensitive() {
        TopicFilter filter = new TopicFilter(List.of(), List.of("Internal.*"));
        assertTrue(filter.isExcluded("Internal.x"));
        assertFalse(filter.isExcluded("internal.x"));
    }
}
