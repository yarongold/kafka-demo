package dev.yarongold.kafkademo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.yarongold.kafkademo.common.OrderJackson;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return OrderJackson.createMapper();
    }
}
