package com.example.app1.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component // spring managed bean for @KafkaListener to work & Consumer configuration must have @EnableKafka annotation
public class MyCustomTopicListener {

    @KafkaListener(topics = "myCustom", groupId = "foo")
    public void listenGroupFoo(String message) {
        // reads all the messages received after it starts (not reading older ones)
        System.out.println("Received Message in group foo: " + message);
    }

    @KafkaListener(topics = "myCustom", groupId = "foo2")
    public void listenGroupFoo2(String message) {
        // reads all the messages received after it starts (not reading older ones)
        System.out.println("Another listener for same topic with group foo2: " + message);
    }
}
