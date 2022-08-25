package com.example.app1.consumer;

import com.example.app1.KafkaTopicConfig;
import com.example.app1.MessagePOJO;
import com.example.app1.producer.KafkaProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Configuration
public class KafkaConsumerConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class.getName());
    @Value(value = "${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    public Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public ReceiverOptions<String, String> receiverOptions(){
        return ReceiverOptions.create(consumerConfig());
    }

    public /*Disposable*/void consumeMessages() {consumeMessages(KafkaTopicConfig.MY_CUSTOM_TOPIC);}

    public /*Disposable*/void consumeMessages(String topic/*, CountDownLatch latch*/) {
        ReceiverOptions<String, String> options = receiverOptions().subscription(Collections.singleton(topic))
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

        // listen until countdownlatch ends
//        Flux<ReceiverRecord<String, String>> kafkaFlux = KafkaReceiver.create(options).receive();
//        return kafkaFlux.subscribe(record -> {
//            ReceiverOffset offset = record.receiverOffset();
//            Instant timestamp = Instant.ofEpochMilli(record.timestamp());
//            System.out.printf("Received message: topic-partition=%s offset=%s timestamp=%s key=%d value=%s\n",
//                    offset.topicPartition(),
//                    offset.offset(),
//                    DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy").format(timestamp),
//                    record.key(),
//                    record.value());
//            offset.acknowledge();
////            latch.countDown();
//        });

        // listen forever
        KafkaReceiver kafkaReceiver = KafkaReceiver.create(options);
        ((Flux<ReceiverRecord>) kafkaReceiver.receive())
                .doOnNext(record -> {
                    System.out.println(record.value());
                    record.receiverOffset().acknowledge();
                })
                .subscribe();
    }
}
