package com.treblle.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TreblleConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("treblle.excluded.topics");
        System.clearProperty("treblle.sdk.token");
        System.clearProperty("treblle.capture.record.keys");
    }

    @Test
    void defaultMaskedKeywordsAlwaysApply() {
        TreblleConfig config = TreblleConfig.builder().maskedKeywords("Email").build();
        assertTrue(config.getMaskedKeywords().contains("password"));
        assertTrue(config.getMaskedKeywords().contains("authorization"));
        assertTrue(config.getMaskedKeywords().contains("email"), "lowercased");

        TreblleConfig none = TreblleConfig.builder().maskedKeywords().build();
        assertTrue(none.getMaskedKeywords().containsAll(
                TreblleConfig.DEFAULT_MASKED_KEYWORDS));
    }

    @Test
    void explicitEmptyListWinsOverSystemProperty() {
        System.setProperty("treblle.excluded.topics", "orders.*");
        assertEquals(1, TreblleConfig.builder().build().getExcludedTopics().size());
        assertTrue(TreblleConfig.builder().excludedTopics().build().getExcludedTopics().isEmpty());
    }

    @Test
    void builderWinsOverSystemProperty() {
        System.setProperty("treblle.sdk.token", "from-property");
        assertEquals("from-builder",
                TreblleConfig.builder().sdkToken("from-builder").build().getSdkToken());
    }

    @Test
    void recordKeyCaptureDefaultsOnAndCanBeDisabled() {
        assertTrue(TreblleConfig.builder().build().isCaptureRecordKeys());
        System.setProperty("treblle.capture.record.keys", "false");
        assertFalse(TreblleConfig.builder().build().isCaptureRecordKeys());
    }

    @Test
    void describeNeverPrintsCredentials() {
        String described = TreblleConfig.builder()
                .sdkToken("supersecrettoken").apiKey("supersecretkey").build().describe();
        assertFalse(described.contains("supersecrettoken"));
        assertFalse(described.contains("supersecretkey"));
    }
}
