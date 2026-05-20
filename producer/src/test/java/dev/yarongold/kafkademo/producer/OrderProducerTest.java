package dev.yarongold.kafkademo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.yarongold.kafkademo.common.Order;
import dev.yarongold.kafkademo.common.OrderJackson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "PRODUCER_RATE_MS=200"
)
@EmbeddedKafka(partitions = 3, topics = {"orders"})
@DirtiesContext
class OrderProducerTest {

    private static final Pattern CUST_KEY = Pattern.compile("cust-[0-9]");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private OrderProducer orderProducer;

    @Test
    void producesOrdersWithCustomerIdAsKey() {
        Map<String, Object> consumerProps = new HashMap<>(
                KafkaTestUtils.consumerProps("test-group-a", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = OrderJackson.createMapper();
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Set.of("orders"));

            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && received < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    assertThat(rec.key()).matches(CUST_KEY);
                    Order order;
                    try {
                        order = mapper.readValue(rec.value(), Order.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    assertThat(order.orderId()).isNotNull();
                    assertThat(order.customerId()).isNotNull();
                    assertThat(order.amount()).isNotNull();
                    assertThat(order.createdAt()).isNotNull();
                    received++;
                }
            }
        }

        assertThat(received).isGreaterThanOrEqualTo(2);
    }

    @Test
    void everySeventhOrderIsBad() {
        // Drive the emit method directly. The scheduler may also be running concurrently,
        // so we identify our 14 emissions by the orderId range and verify that within
        // that contiguous window, exactly orders with n % 7 == 0 are bad and others are good.
        Map<String, Object> consumerProps = new HashMap<>(
                KafkaTestUtils.consumerProps("test-group-b", "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton("orders"));

            // Direct calls — counter increments before use, so first call yields ORD-1.
            for (int i = 0; i < 14; i++) {
                orderProducer.emit();
            }

            ObjectMapper mapper = OrderJackson.createMapper();
            Map<Long, Order> byN = new HashMap<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    Order order;
                    try {
                        order = mapper.readValue(rec.value(), Order.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    long n = Long.parseLong(order.orderId().substring("ORD-".length()));
                    byN.put(n, order);
                }
                // We need at least 14 contiguous records covering at least one multiple-of-7
                if (byN.size() >= 14 && hasContiguousWindowWithTwoBad(byN)) {
                    break;
                }
            }

            // Validate cadence in any contiguous window of 14 from our captured records.
            assertThat(byN.size()).as("expected at least 14 records").isGreaterThanOrEqualTo(14);

            int bad = 0;
            int good = 0;
            for (Map.Entry<Long, Order> entry : byN.entrySet()) {
                long n = entry.getKey();
                Order order = entry.getValue();
                if (n % 7 == 0) {
                    assertThat(order.amount().signum())
                            .as("order ORD-%d should be the bad record (amount < 0)", n)
                            .isLessThan(0);
                    bad++;
                } else {
                    assertThat(order.amount().signum())
                            .as("order ORD-%d should be a good record (amount > 0)", n)
                            .isGreaterThan(0);
                    good++;
                }
            }

            assertThat(bad).as("at least one bad record (n % 7 == 0) should have been observed").isGreaterThanOrEqualTo(1);
            assertThat(good).as("at least one good record should have been observed").isGreaterThanOrEqualTo(1);
        }
    }

    private static boolean hasContiguousWindowWithTwoBad(Map<Long, Order> byN) {
        long min = byN.keySet().stream().mapToLong(Long::longValue).min().orElse(0);
        long max = byN.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
        for (long start = min; start + 13 <= max; start++) {
            boolean contiguous = true;
            int bad = 0;
            for (long n = start; n < start + 14; n++) {
                if (!byN.containsKey(n)) {
                    contiguous = false;
                    break;
                }
                if (n % 7 == 0) bad++;
            }
            if (contiguous && bad == 2) return true;
        }
        return false;
    }
}
