package com.example.app1.producer;

import com.example.app1.KafkaTopicConfig;
import com.example.app1.MessagePOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    private final Logger log = LoggerFactory.getLogger(Controller.class);
    private final ReactiveKafkaProducerTemplate<String, MessagePOJO> reactiveKafkaProducerTemplate;

    public Controller(ReactiveKafkaProducerTemplate<String, MessagePOJO> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    @PostMapping("/send")
    public void send(@RequestBody MessagePOJO msg){
        log.info("send to topic={}, {}={},", KafkaTopicConfig.MY_CUSTOM_TOPIC, MessagePOJO.class.getSimpleName(), msg);
        reactiveKafkaProducerTemplate.send(KafkaTopicConfig.MY_CUSTOM_TOPIC, msg)
                .doOnSuccess(senderResult -> log.info("sent {} offset : {}", msg, senderResult.recordMetadata().offset()))
                .subscribe();
    }
}
