package com.treblle.kafka.internal.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class JsonParserTest {

    @Test
    void parsesAllValueTypes() {
        Object parsed = JsonParser.parse(
                "{\"s\":\"a\\u00e9\\n\",\"i\":-12,\"d\":1.5e3,\"t\":true,\"f\":false,\"n\":null,"
                        + "\"a\":[1,{}]}");
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("s", "aé\n");
        expected.put("i", -12L);
        expected.put("d", 1500.0);
        expected.put("t", true);
        expected.put("f", false);
        expected.put("n", null);
        expected.put("a", List.of(1L, Map.of()));
        assertEquals(expected, parsed);
    }

    @Test
    void rejectsNonRfcNumbers() {
        for (String bad : Arrays.asList("+1", "01", "1.", ".5", "1e", "--1", "1.2.3", "0x10", "NaN")) {
            assertFalse(JsonParser.isValid(bad), bad);
        }
        for (String good : Arrays.asList("0", "-0", "10", "1.25", "1e10", "1E-2", "-3.5e+2")) {
            assertTrue(JsonParser.isValid(good), good);
        }
    }

    @Test
    void widensHugeIntegersToDouble() {
        assertEquals(1.0e20, JsonParser.parse("100000000000000000000"));
    }

    @Test
    void rejectsTrailingContentAndDeepNesting() {
        assertThrows(JsonException.class, () -> JsonParser.parse("{} {}"));
        assertThrows(JsonException.class, () -> JsonParser.parse("[".repeat(100) + "]".repeat(100)));
    }

    @Test
    void writerOutputRoundTrips() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("text", "quote\" backslash\\ control\u0001");
        value.put("list", List.of(1L, 2.5, true));
        value.put("nan", Double.NaN);
        String json = JsonWriter.write(value);
        Map<?, ?> parsed = (Map<?, ?>) JsonParser.parse(json);
        assertEquals(value.get("text"), parsed.get("text"));
        assertEquals(value.get("list"), parsed.get("list"));
        assertEquals(null, parsed.get("nan"));
    }

    @Test
    void writerOutputForDeepStructuresIsAlwaysParseable() {
        Object deep = "leaf";
        for (int i = 0; i < 200; i++) {
            deep = List.of(deep);
        }
        assertTrue(JsonParser.isValid(JsonWriter.write(deep)));
    }
}
