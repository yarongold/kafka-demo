package dev.yarongold.kafkademo.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

class OrderJacksonTest {

    private final ObjectMapper mapper = OrderJackson.createMapper();

    private Order sampleOrder() {
        return new Order(
                "order-123",
                "customer-456",
                new BigDecimal("19.95"),
                Instant.now().truncatedTo(ChronoUnit.MICROS));
    }

    @Test
    void roundTripsOrderToEqualInstance() throws Exception {
        Order original = sampleOrder();
        String json = mapper.writeValueAsString(original);
        Order roundTripped = mapper.readValue(json, Order.class);
        assertEquals(original, roundTripped);
    }

    @Test
    void serializesInstantAsIso8601String() throws Exception {
        Order original = sampleOrder();
        String json = mapper.writeValueAsString(original);
        JsonNode node = mapper.readTree(json);
        JsonNode createdAt = node.get("createdAt");
        assertTrue(createdAt.isTextual(), "createdAt should be a JSON string, was: " + createdAt);
        String text = createdAt.asText();
        assertTrue(Character.isDigit(text.charAt(0)), "ISO-8601 timestamp should start with a digit: " + text);
        assertTrue(text.contains("T"), "ISO-8601 timestamp should contain 'T': " + text);
        assertTrue(text.contains("Z"), "ISO-8601 timestamp should contain 'Z': " + text);
    }

    @Test
    void serializesBigDecimalAsJsonNumber() throws Exception {
        Order original = sampleOrder();
        String json = mapper.writeValueAsString(original);
        JsonNode node = mapper.readTree(json);
        JsonNode amount = node.get("amount");
        assertTrue(amount.isNumber(), "amount should be a JSON number, was: " + amount);
    }

    @Test
    void serializesFieldNamesInCamelCase() throws Exception {
        Order original = sampleOrder();
        String json = mapper.writeValueAsString(original);
        assertTrue(json.contains("\"orderId\""), "JSON should contain \"orderId\": " + json);
        assertTrue(json.contains("\"customerId\""), "JSON should contain \"customerId\": " + json);
        assertTrue(json.contains("\"amount\""), "JSON should contain \"amount\": " + json);
        assertTrue(json.contains("\"createdAt\""), "JSON should contain \"createdAt\": " + json);
    }
}
