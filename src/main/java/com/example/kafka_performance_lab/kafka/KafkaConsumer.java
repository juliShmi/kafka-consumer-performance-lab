package com.example.kafka_performance_lab.kafka;

import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaConsumer {

    private static final String PRODUCED_AT_HEADER = "producedAtEpochMs";

    private final Timer processingTimer;
    private final Timer endToEndTimer;

    public KafkaConsumer(MeterRegistry meterRegistry) {
        this.processingTimer = Timer.builder("kafka_consumer_processing_seconds")
                .description("Time spent processing a Kafka message inside the consumer")
                .publishPercentileHistogram(true)
                .register(meterRegistry);

        this.endToEndTimer = Timer.builder("kafka_consumer_end_to_end_seconds")
                .description("End-to-end latency from producer timestamp header to consumer processing time")
                .publishPercentileHistogram(true)
                .register(meterRegistry);
    }

    @KafkaListener(topics = "order-create")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws InterruptedException {
        var sample = Timer.start();
        try {
            var producedAt = record.headers().lastHeader(PRODUCED_AT_HEADER);
            if (producedAt != null && producedAt.value() != null && producedAt.value().length == Long.BYTES) {
                long producedAtMs = ByteBuffer.wrap(producedAt.value()).getLong();
                long e2eMs = Math.max(0, System.currentTimeMillis() - producedAtMs);
                endToEndTimer.record(e2eMs, TimeUnit.MILLISECONDS);
            }

            Thread.sleep(100);
            log.info("Processed {}", record.value());
            acknowledgment.acknowledge();
        } finally {
            sample.stop(processingTimer);
        }
    }
}
