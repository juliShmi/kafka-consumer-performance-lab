package com.example.kafka_performance_lab.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.example.kafka_performance_lab.service.OrderProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaConsumer {

    private static final String PRODUCED_AT_HEADER = "producedAtEpochMs";
    private static final String POISON_PREFIX = "POISON:";
    private static final String IRRELEVANT_MARKER = "irrelevant";

    private final Timer processingTimer;
    private final Timer endToEndTimer;
    private final Counter relevantEventsCounter;
    private final Counter irrelevantEventsCounter;
    private final OrderProcessingService orderProcessingService;

    public KafkaConsumer(MeterRegistry meterRegistry, OrderProcessingService orderProcessingService) {
        this.processingTimer = Timer.builder("kafka_consumer_processing_seconds")
                .description("Time spent processing a Kafka message inside the consumer")
                .publishPercentileHistogram(true)
                .register(meterRegistry);

        this.endToEndTimer = Timer.builder("kafka_consumer_end_to_end_seconds")
                .description("End-to-end latency from producer timestamp header to consumer processing time")
                .publishPercentileHistogram(true)
                .register(meterRegistry);

        this.relevantEventsCounter = Counter.builder("kafka_consumer_events_total")
                .description("Number of events received by consumer by relevance")
                .tag("type", "relevant")
                .register(meterRegistry);

        this.irrelevantEventsCounter = Counter.builder("kafka_consumer_events_total")
                .description("Number of events received by consumer by relevance")
                .tag("type", "irrelevant")
                .register(meterRegistry);

        this.orderProcessingService = orderProcessingService;
    }

    @KafkaListener(topics = "order-create")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws InterruptedException {
        var sample = Timer.start();
        try {
            if (record.value() != null && record.value().startsWith(POISON_PREFIX)) {
                throw new IllegalArgumentException("Poison pill message");
            }

            if (record.value() != null && record.value().toLowerCase().contains(IRRELEVANT_MARKER)) {
                irrelevantEventsCounter.increment();
                acknowledgment.acknowledge();
                return;
            }

            relevantEventsCounter.increment();

            var producedAt = record.headers().lastHeader(PRODUCED_AT_HEADER);
            if (producedAt != null && producedAt.value() != null && producedAt.value().length == Long.BYTES) {
                long producedAtMs = ByteBuffer.wrap(producedAt.value()).getLong();
                long e2eMs = Math.max(0, System.currentTimeMillis() - producedAtMs);
                endToEndTimer.record(e2eMs, TimeUnit.MILLISECONDS);
            }

            orderProcessingService.processAndStageOutbox(record);
            log.info("Processed relevant key={} value={}", record.key(), record.value());
            acknowledgment.acknowledge();
        } finally {
            sample.stop(processingTimer);
        }
    }

    @KafkaListener(topics = "order-create.DLT", groupId = "orders-group-dlt")
    public void listenDlt(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.warn("DLT message received. topic={}, partition={}, offset={}, value={}",
                    record.topic(), record.partition(), record.offset(), record.value());
            acknowledgment.acknowledge();
        } catch (RuntimeException e) {
            log.error("Failed to handle DLT message; leaving unacked for retry", e);
            throw e;
        }
    }
}
