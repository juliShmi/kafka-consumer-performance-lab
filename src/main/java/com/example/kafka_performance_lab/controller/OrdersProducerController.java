package com.example.kafka_performance_lab.controller;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.nio.ByteBuffer;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrdersProducerController {

    private static final String TOPIC = "order-create";
    private static final String PRODUCED_AT_HEADER = "producedAtEpochMs";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrdersProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/generate")
    public String generateOrders(@RequestParam(defaultValue = "10000") int count) {
        for (int i = 0; i < count; i++) {
            var record = new ProducerRecord<String, String>(TOPIC, UUID.randomUUID().toString());
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array()));
            kafkaTemplate.send(record);
        }
        return "Sent " + count + " messages";
    }
}
