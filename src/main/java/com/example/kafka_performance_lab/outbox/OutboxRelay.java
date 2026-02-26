package com.example.kafka_performance_lab.outbox;

import com.example.kafka_performance_lab.db.OutboxEventEntity;
import com.example.kafka_performance_lab.db.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final String OUTBOX_ID_HEADER = "outboxId";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;
    private final Duration inFlightTimeout;

    public OutboxRelay(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.outbox.publish.batch-size:100}") int batchSize,
            @Value("${app.outbox.in-flight-timeout-ms:60000}") long inFlightTimeoutMs
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
        this.inFlightTimeout = Duration.ofMillis(Math.max(1, inFlightTimeoutMs));
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish.fixed-delay-ms:250}")
    public void publishPending() {
        releaseTimedOutInFlight();
        List<UUID> claimed = claimNextBatch();
        for (UUID id : claimed) {
            publishOne(id);
        }
    }

    @Transactional
    protected void releaseTimedOutInFlight() {
        Instant cutoff = Instant.now().minus(inFlightTimeout);
        int released = outboxEventRepository.releaseTimedOutInFlight(cutoff, "in-flight timeout");
        if (released > 0) {
            log.warn("Released {} timed-out IN_FLIGHT outbox events", released);
        }
    }

    @Transactional
    protected List<UUID> claimNextBatch() {
        List<OutboxEventEntity> batch = outboxEventRepository.lockNextBatch(batchSize);
        Instant now = Instant.now();
        for (OutboxEventEntity event : batch) {
            event.markInFlight(now);
        }
        return batch.stream().map(OutboxEventEntity::getId).toList();
    }

    private void publishOne(UUID id) {
        OutboxEventEntity event = load(id);
        if (event == null) return;

        try {
            var record = new ProducerRecord<>(event.getDestinationTopic(), event.getDestinationKey(), event.getPayload());
            record.headers().add(new RecordHeader(OUTBOX_ID_HEADER, event.getId().toString().getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).get();
            markSent(id);
        } catch (Exception e) {
            markFailed(id, e);
            log.warn(
                    "Failed to publish outbox event id={}, attempts={}, nextAttemptInMs={}, sourcePartition={}, sourceOffset={}",
                    event.getId(),
                    event.getAttempts() + 1,
                    computeBackoff(event.getAttempts() + 1).toMillis(),
                    event.getSourcePartition(),
                    event.getSourceOffset(),
                    e
            );
        }
    }

    @Transactional
    protected void markSent(UUID id) {
        OutboxEventEntity event = outboxEventRepository.findById(id).orElse(null);
        if (event == null) return;
        event.markSent(Instant.now());
    }

    @Transactional
    protected void markFailed(UUID id, Exception e) {
        OutboxEventEntity event = outboxEventRepository.findById(id).orElse(null);
        if (event == null) return;

        int nextAttempts = event.getAttempts() + 1;
        Duration backoff = computeBackoff(nextAttempts);
        Instant nextAttemptAt = Instant.now().plus(backoff);
        event.markAttemptFailed(nextAttempts, nextAttemptAt, abbreviate(e.toString(), 4_000));
        event.releaseToPending(nextAttemptAt, event.getLastError());
    }

    @Transactional
    protected OutboxEventEntity load(UUID id) {
        return outboxEventRepository.findById(id).orElse(null);
    }

    private static Duration computeBackoff(int nextAttempts) {
        return Duration.ofMillis(Math.min(30_000L, 250L * (1L << Math.min(10, nextAttempts))));
    }

    private static String abbreviate(String s, int maxLen) {
        if (s == null || s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen);
    }
}

