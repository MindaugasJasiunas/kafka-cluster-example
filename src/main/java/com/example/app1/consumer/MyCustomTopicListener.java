package com.example.app1.consumer;

//import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MyCustomTopicListener {
    private final KafkaConsumerConfig consumer;

    public MyCustomTopicListener(KafkaConsumerConfig consumer) {
        this.consumer = consumer;
        consumer.consumeMessages();
    }

//    @KafkaListener(topics = "myCustom", groupId = "foo")
//    public void listenGroupFoo(String message) {
//        // reads all the messages received after it starts (not reading older ones)
//        System.out.println("Received Message in group foo: " + message);
//    }
//
//    @KafkaListener(topics = "myCustom", groupId = "foo2")
//    public void listenGroupFoo2(String message) {
//        // reads all the messages received after it starts (not reading older ones)
//        System.out.println("Another listener for same topic with group foo2: " + message);
//    }
}
