package dev.yarongold.kafkademo.producer;

import dev.yarongold.kafkademo.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC = "orders";
    private static final int CUSTOMER_POOL_SIZE = 10;

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final AtomicLong counter = new AtomicLong(0);

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRateString = "${PRODUCER_RATE_MS:1000}")
    void emit() {
        long n = counter.incrementAndGet();
        String orderId = "ORD-" + n;
        String customerId = "cust-" + ThreadLocalRandom.current().nextInt(CUSTOMER_POOL_SIZE);

        BigDecimal amount;
        if (n % 7 == 0) {
            amount = BigDecimal.valueOf(-1);
        } else {
            double raw = 1.00 + ThreadLocalRandom.current().nextDouble() * (999.99 - 1.00);
            amount = BigDecimal.valueOf(raw).setScale(2, RoundingMode.HALF_UP);
        }

        Order order = new Order(orderId, customerId, amount, Instant.now());

        kafkaTemplate.send(TOPIC, customerId, order).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("failed to send orderId={} customerId={}", orderId, customerId, ex);
            } else {
                var metadata = result.getRecordMetadata();
                log.info("sent orderId={} customerId={} → partition={} offset={}",
                        orderId, customerId, metadata.partition(), metadata.offset());
            }
        });
    }
}
