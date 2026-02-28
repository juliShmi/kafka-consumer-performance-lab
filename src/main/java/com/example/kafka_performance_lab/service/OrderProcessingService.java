package com.example.kafka_performance_lab.service;

import com.example.kafka_performance_lab.db.OrderEntity;
import com.example.kafka_performance_lab.db.OrderRepository;
import com.example.kafka_performance_lab.db.OutboxEventEntity;
import com.example.kafka_performance_lab.db.OutboxEventRepository;
import com.example.kafka_performance_lab.db.OutboxStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderProcessingService {
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final String outboxDestinationTopic;
    private final long processingDelayMs;

    public OrderProcessingService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository,
            @Value("${app.outbox.publish.topic:order-create.processed}") String outboxDestinationTopic,
            @Value("${app.processing.delay-ms:100}") long processingDelayMs
    ) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.outboxDestinationTopic = outboxDestinationTopic;
        this.processingDelayMs = processingDelayMs;
    }

    @Transactional
    public void processAndStageOutbox(ConsumerRecord<String, String> record) throws InterruptedException {
        if (record.key() == null || record.key().isBlank()) {
            throw new IllegalArgumentException("Record key is required for ordering/partitioning");
        }

        var now = Instant.now();

        try {
            orderRepository.save(new OrderEntity(
                    UUID.randomUUID(),
                    record.key(),
                    record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    now
            ));

            if (processingDelayMs > 0) {
                Thread.sleep(processingDelayMs);
            }

            outboxEventRepository.save(new OutboxEventEntity(
                    UUID.randomUUID(),
                    outboxDestinationTopic,
                    record.key(),
                    "PROCESSED:" + record.value(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    OutboxStatus.PENDING,
                    0,
                    now,
                    null,
                    null,
                    now,
                    null
            ));
        } catch (DataIntegrityViolationException e) {
            
        }
    }
}

