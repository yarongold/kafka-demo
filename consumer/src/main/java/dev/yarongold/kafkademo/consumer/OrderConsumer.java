package dev.yarongold.kafkademo.consumer;

import dev.yarongold.kafkademo.common.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);
    private static final String TOPIC = "orders";

    private final String podName;

    public OrderConsumer() {
        String hostname = System.getenv("HOSTNAME");
        this.podName = (hostname == null || hostname.isBlank()) ? "local" : hostname;
    }

    @KafkaListener(topics = TOPIC, groupId = "demo-consumer-group", concurrency = "3")
    public void consume(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        if (order.amount().signum() <= 0) {
            throw new InvalidOrderException(
                    "invalid order orderId=" + order.orderId() + " amount=" + order.amount());
        }

        log.info("processed orderId={} partition={} offset={} podName={}",
                order.orderId(), partition, offset, podName);
    }
}
