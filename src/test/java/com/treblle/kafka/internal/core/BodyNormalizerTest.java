package com.treblle.kafka.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class BodyNormalizerTest {

    @Test
    void parsesJsonText() {
        Map<?, ?> body = (Map<?, ?>) BodyNormalizer.normalize("{\"a\":1}");
        assertEquals(1L, body.get("a"));
    }

    @Test
    void parsesJsonBytesAndBuffers() {
        byte[] bytes = "[1,2]".getBytes(StandardCharsets.UTF_8);
        assertEquals(List.of(1L, 2L), BodyNormalizer.normalize(bytes));
        assertEquals(List.of(1L, 2L), BodyNormalizer.normalize(ByteBuffer.wrap(bytes)));
    }

    @Test
    void keepsTombstonesAsNull() {
        assertNull(BodyNormalizer.normalize(null));
    }

    @Test
    void describesBinaryPayloads() {
        Map<?, ?> body = (Map<?, ?>) BodyNormalizer.normalize(new byte[] {0, 1, 2, (byte) 0xff});
        assertEquals("application/octet-stream", body.get("type"));
    }

    @Test
    void replacesOversizedText() {
        String big = "x".repeat(BodyNormalizer.MAX_BODY_BYTES + 1);
        Map<?, ?> body = (Map<?, ?>) BodyNormalizer.normalize(big);
        assertTrue(body.get("message").toString().contains("too large"));
    }

    @Test
    void replacesOversizedMaps() {
        Map<String, Object> big = new HashMap<>();
        for (int i = 0; i < 30_000; i++) {
            big.put("key-" + i, "value-value-value-value-value-value-value-value-value-" + i);
        }
        Map<?, ?> body = (Map<?, ?>) BodyNormalizer.normalize(big);
        assertTrue(body.get("message").toString().contains("too large"));
    }

    @Test
    void copiesStructuresIntoPlainJdkTypes() {
        List<Object> list = new ArrayList<>(List.of("a", Collections.singletonMap("b", 2)));
        Object copy = BodyNormalizer.normalize(list);
        assertEquals(List.of("a", Map.of("b", 2L)), copy);
        list.add("c");
        assertEquals(2, ((List<?>) copy).size(), "the copy is detached from the original");
    }

    @Test
    void cyclicStructuresDoNotBlowUp() {
        Map<String, Object> cyclic = new HashMap<>();
        cyclic.put("self", cyclic);
        assertNotNull(BodyNormalizer.normalize(cyclic));
    }

    @Test
    void oversizedPlaceholderIsCheapAndExact() {
        assertNull(BodyNormalizer.oversizedPlaceholder("small"));
        assertNull(BodyNormalizer.oversizedPlaceholder(new Object()));
        assertNotNull(BodyNormalizer.oversizedPlaceholder(new byte[BodyNormalizer.MAX_BODY_BYTES + 1]));
    }

    @Test
    void estimatesSize() {
        assertEquals(0, BodyNormalizer.estimateBytes(null));
        assertEquals(10, BodyNormalizer.estimateBytes(new byte[10]));
        assertEquals(BodyNormalizer.NOMINAL_OBJECT_BYTES, BodyNormalizer.estimateBytes(Map.of()));
    }
}
