package com.example.kafka_performance_lab.db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {
    @Id
    private UUID id;

    @Column(name = "destination_topic", nullable = false, length = 512)
    private String destinationTopic;

    @Column(name = "destination_key", nullable = false, length = 512)
    private String destinationKey;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "source_topic", nullable = false, length = 512)
    private String sourceTopic;

    @Column(name = "source_partition", nullable = false)
    private int sourcePartition;

    @Column(name = "source_offset", nullable = false)
    private long sourceOffset;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OutboxStatus status;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "locked_at")
    private Instant lockedAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEventEntity() {}

    public OutboxEventEntity(
            UUID id,
            String destinationTopic,
            String destinationKey,
            String payload,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            OutboxStatus status,
            int attempts,
            Instant nextAttemptAt,
            Instant lockedAt,
            String lastError,
            Instant createdAt,
            Instant publishedAt
    ) {
        this.id = id;
        this.destinationTopic = destinationTopic;
        this.destinationKey = destinationKey;
        this.payload = payload;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.status = status;
        this.attempts = attempts;
        this.nextAttemptAt = nextAttemptAt;
        this.lockedAt = lockedAt;
        this.lastError = lastError;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
    }

    public UUID getId() {
        return id;
    }

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public String getDestinationKey() {
        return destinationKey;
    }

    public String getPayload() {
        return payload;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public int getSourcePartition() {
        return sourcePartition;
    }

    public long getSourceOffset() {
        return sourceOffset;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public Instant getLockedAt() {
        return lockedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void markSent(Instant when) {
        this.status = OutboxStatus.SENT;
        this.publishedAt = when;
        this.lockedAt = null;
        this.lastError = null;
    }

    public void markAttemptFailed(int attempts, Instant nextAttemptAt, String lastError) {
        this.attempts = attempts;
        this.nextAttemptAt = nextAttemptAt;
        this.lastError = lastError;
    }

    public void markInFlight(Instant when) {
        this.status = OutboxStatus.IN_FLIGHT;
        this.lockedAt = when;
    }

    public void releaseToPending(Instant nextAttemptAt, String lastError) {
        this.status = OutboxStatus.PENDING;
        this.lockedAt = null;
        this.nextAttemptAt = nextAttemptAt;
        this.lastError = lastError;
    }
}
