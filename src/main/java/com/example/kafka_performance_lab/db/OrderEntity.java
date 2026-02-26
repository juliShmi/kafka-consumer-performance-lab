package com.example.kafka_performance_lab.db;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {
    @Id
    private UUID id;

    @Column(name = "order_key", nullable = false, length = 512)
    private String orderKey;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "source_topic", nullable = false, length = 512)
    private String sourceTopic;

    @Column(name = "source_partition", nullable = false)
    private int sourcePartition;

    @Column(name = "source_offset", nullable = false)
    private long sourceOffset;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected OrderEntity() {}

    public OrderEntity(
            UUID id,
            String orderKey,
            String payload,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Instant createdAt
    ) {
        this.id = id;
        this.orderKey = orderKey;
        this.payload = payload;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getOrderKey() {
        return orderKey;
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

    public Instant getCreatedAt() {
        return createdAt;
    }
}
