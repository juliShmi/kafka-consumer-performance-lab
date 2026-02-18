package com.example.kafka_performance_lab.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrdersProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrdersProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/generate")
    public String generateOrders(@RequestParam(defaultValue = "1000") int count) {
        for (int i = 0; i < count; i++) {
            kafkaTemplate.send("order-create", UUID.randomUUID().toString());
        }
        return "Sent " + count + " messages";
    }
}
