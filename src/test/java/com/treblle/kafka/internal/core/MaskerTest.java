package com.treblle.kafka.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class MaskerTest {

    private final Masker masker = new Masker(Set.of("password", "authorization", "secrets", "ssn"));

    @Test
    void masksNestedValuesCaseInsensitively() {
        Object masked = masker.maskBody(Map.of(
                "customer", Map.of("password", "hunter123", "SSN", "123-45-6789"),
                "secrets", List.of("alpha", "bravo")));
        assertEquals(Map.of(
                "customer", Map.of("password", "*********", "SSN", "***********"),
                "secrets", List.of("*****", "*****")), masked);
    }

    @Test
    void keepsAuthSchemeInHeaders() {
        Map<String, String> masked = masker.maskHeaders(Map.of("Authorization", "Bearer abc123"));
        assertEquals("Bearer ******", masked.get("Authorization"));
    }

    @Test
    void scrubsKeyValuePairsInText() {
        assertEquals("bad password=***** for user",
                masker.maskText("bad password=hunt3 for user"));
        assertEquals("{\"password\": \"*****\"}", masker.maskText("{\"password\": \"hunt3\"}"));
        assertEquals("Authorization: Bearer ******",
                masker.maskText("Authorization: Bearer abc123"));
        assertEquals("passwords are not keys", masker.maskText("passwords are not keys"));
        assertEquals("no secrets here", masker.maskText("no secrets here"));
    }
}
