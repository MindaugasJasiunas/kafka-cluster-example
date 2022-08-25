package com.example.app1.consumer;

import com.example.app1.KafkaTopicConfig;
import com.example.app1.MessagePOJO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    public Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TYPE_MAPPINGS, "MessagePOJO:com.example.app1.MessagePOJO");  // The class 'com.example.app1.MessagePOJO' is not in the trusted packages: [java.util, java.lang]. If you believe this class is safe to deserialize, please provide its name. If the serialization is only done by a trusted source, you can also enable trust all (*).
        return props;
    }

    @Bean
    public ReceiverOptions<String, MessagePOJO> kafkaReceiverOptions() {
        ReceiverOptions<String, MessagePOJO> basicReceiverOptions = ReceiverOptions.create(consumerConfig());
        return basicReceiverOptions.subscription(Collections.singletonList(KafkaTopicConfig.MY_CUSTOM_TOPIC));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, MessagePOJO> reactiveKafkaConsumerTemplate(ReceiverOptions<String, MessagePOJO> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<String, MessagePOJO>(kafkaReceiverOptions);
    }

}
