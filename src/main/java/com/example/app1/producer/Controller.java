package com.example.app1.producer;

import com.example.app1.MessagePOJO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    private final KafkaTemplate<String, MessagePOJO> kafkaTemplate;

    public Controller(KafkaTemplate<String, MessagePOJO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public void send(@RequestBody MessagePOJO msg){
//      this.kafkaTemplate.send("topic", "key", "message"); // provide key if using partitioning in Kafka
        this.kafkaTemplate.send("myCustom", msg);
    }
}
