package com.example.app1.producer;

import com.example.app1.KafkaTopicConfig;
import com.example.app1.MessagePOJO;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class.getName());
    @Value(value = "${spring.kafka.bootstrap-servers}")
    String bootstrapServers;
    private KafkaSender<String, String> sender;

    public Map<String, Object> producerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class); //send JSON
        return props;
    }

    public SenderOptions<String, String> senderOptions(){
        return SenderOptions.create(producerConfig());
    }

    @Bean
    public KafkaSender<String, String> kafkaTemplate(){
        sender = KafkaSender.create(senderOptions());
        return sender;
    }

    public void close() {
        sender.close();
    }

    public void sendMessage(String message){sendMessage(message, KafkaTopicConfig.MY_CUSTOM_TOPIC);}

    public void sendMessage(String message, String topic){
        sender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, message), message)))
                .doOnError(throwable -> log.error(throwable.getMessage()))
                .subscribe(result -> {
                    RecordMetadata metadata = result.recordMetadata();
                    Instant timestamp = Instant.ofEpochMilli(metadata.timestamp());
//                    System.out.printf("Message %s sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                    System.out.printf("Message %s sent successfully, topic-partition=%s-%s offset=%d\n",
                            result.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset());
//                            DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy").format(timestamp));
                });
    }
}
