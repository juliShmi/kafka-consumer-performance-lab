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
    private static final String POISON_PREFIX = "POISON:";
    private static final String IRRELEVANT_MARKER = "irrelevant";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrdersProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/generate")
    public String generateOrders(
            @RequestParam(defaultValue = "10000") int count,
            @RequestParam(defaultValue = "100") int keySpace
    ) {
        for (int i = 0; i < count; i++) {
            var key = "order-key-" + (Math.floorMod(i, Math.max(1, keySpace)));
            var value = "OK:key=" + key + ";id=" + UUID.randomUUID();
            var record = new ProducerRecord<>(TOPIC, key, value);
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array()));
            kafkaTemplate.send(record);
        }
        return "Sent " + count + " messages";
    }

    @PostMapping("/generate-test")
    public String generateTestData(
            @RequestParam(defaultValue = "1000") int goodCount,
            @RequestParam(defaultValue = "10") int poisonCount,
            @RequestParam(defaultValue = "20") int irrelevantCount) {

        for (int i = 0; i < goodCount; i++) {
            var key = "good-" + Math.floorMod(i, 10);
            var record = new ProducerRecord<String, String>(TOPIC, key, "OK:" + UUID.randomUUID());
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array()));
            kafkaTemplate.send(record);
        }

        for (int i = 0; i < irrelevantCount; i++) {
            var key = "irrelevant-" + Math.floorMod(i, 10);
            var record = new ProducerRecord<String, String>(TOPIC, key, "event=" + IRRELEVANT_MARKER + ";id=" + UUID.randomUUID());
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array()));
            kafkaTemplate.send(record);
        }

        for (int i = 0; i < poisonCount; i++) {
            var key = "poison-" + Math.floorMod(i, 10);
            var record = new ProducerRecord<String, String>(TOPIC, key, POISON_PREFIX + UUID.randomUUID());
            record.headers().add(new RecordHeader(PRODUCED_AT_HEADER, ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array()));
            kafkaTemplate.send(record);
        }

        return "Sent good=" + goodCount + ", irrelevant=" + irrelevantCount + ", poison=" + poisonCount;
    }
}
