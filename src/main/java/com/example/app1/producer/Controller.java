package com.example.app1.producer;

import com.example.app1.KafkaTopicConfig;
import com.example.app1.MessagePOJO;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

@RestController
public class Controller {
    private final KafkaSender<String, String> kafkaSender;

    public Controller(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @PostMapping("/send")
    public void send(@RequestBody String msg){
        this.kafkaSender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(KafkaTopicConfig.MY_CUSTOM_TOPIC, msg), msg)))
                .doOnError(System.err::println)
                .subscribe(result -> {
                    RecordMetadata metadata = result.recordMetadata();
                    Instant timestamp = Instant.ofEpochMilli(metadata.timestamp());
                    System.out.printf("Message %s sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                            result.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy").format(timestamp));
                });
    }
}
