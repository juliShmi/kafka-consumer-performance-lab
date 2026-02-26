package com.example.kafka_performance_lab.db;

public enum OutboxStatus {
    PENDING,
    IN_FLIGHT,
    SENT
}
