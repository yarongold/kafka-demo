package dev.yarongold.kafkademo.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.yarongold.kafkademo.common.Order;
import dev.yarongold.kafkademo.common.OrderJackson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders-dlq"})
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
class OrderConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void processesValidOrder() {
        Order order = new Order("ORD-100", "cust-1", BigDecimal.valueOf(42.50), Instant.now());

        sendOrder(order);

        // No record should land on orders-dlq for a valid order.
        try (KafkaConsumer<String, String> dlqConsumer = newStringConsumer("dlq-check-" + UUID.randomUUID(), "orders-dlq")) {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.ofMillis(500));
                assertThat(records.count())
                        .as("no DLQ records expected for valid order")
                        .isZero();
            }
        }
    }

    @Test
    void routesInvalidOrderToDlq() throws Exception {
        Order order = new Order("ORD-200", "cust-2", BigDecimal.valueOf(-1), Instant.now());

        sendOrder(order);

        ObjectMapper mapper = OrderJackson.createMapper();
        AtomicReference<ConsumerRecord<String, String>> received = new AtomicReference<>();

        try (KafkaConsumer<String, String> dlqConsumer = newStringConsumer("dlq-reader-" + UUID.randomUUID(), "orders-dlq")) {
            // FixedBackOff(1000ms, 2 retries) → ~3s minimum; give it up to 15s.
            await().atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, String> rec : records) {
                            received.set(rec);
                            return;
                        }
                        assertThat(received.get()).isNotNull();
                    });
        }

        ConsumerRecord<String, String> dlqRecord = received.get();
        assertThat(dlqRecord).as("a DLQ record was expected for the invalid order").isNotNull();

        Order dlqOrder = mapper.readValue(dlqRecord.value(), Order.class);
        assertThat(dlqOrder.orderId()).isEqualTo("ORD-200");

        Header exceptionFqcn = dlqRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN);
        assertThat(exceptionFqcn)
                .as("DLT exception FQCN header should be present")
                .isNotNull();
        // The KafkaListener wraps the thrown exception in ListenerExecutionFailedException,
        // so the original InvalidOrderException FQCN lives in the cause-FQCN header.
        Header exceptionCauseFqcn = dlqRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        String exceptionFqcnValue = new String(exceptionFqcn.value(), StandardCharsets.UTF_8);
        String causeFqcnValue = exceptionCauseFqcn == null
                ? ""
                : new String(exceptionCauseFqcn.value(), StandardCharsets.UTF_8);
        assertThat(exceptionFqcnValue + "|" + causeFqcnValue)
                .as("InvalidOrderException should appear in DLT exception headers")
                .contains("InvalidOrderException");

        Header originalTopic = dlqRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC);
        assertThat(originalTopic)
                .as("DLT original-topic header should be present")
                .isNotNull();
        assertThat(new String(originalTopic.value(), StandardCharsets.UTF_8)).isEqualTo("orders");
    }

    private void sendOrder(Order order) {
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafka));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Disable type headers so the consumer relies on spring.json.value.default.type.
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        try (KafkaProducer<String, Order> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>("orders", order.customerId(), order));
            producer.flush();
        }
    }

    private KafkaConsumer<String, String> newStringConsumer(String groupId, String topic) {
        Map<String, Object> consumerProps = new HashMap<>(
                KafkaTestUtils.consumerProps(groupId, "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Set.of(topic));
        return consumer;
    }
}
