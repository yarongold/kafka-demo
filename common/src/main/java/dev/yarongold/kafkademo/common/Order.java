package dev.yarongold.kafkademo.common;

import java.math.BigDecimal;
import java.time.Instant;

public record Order(String orderId, String customerId, BigDecimal amount, Instant createdAt) { }
