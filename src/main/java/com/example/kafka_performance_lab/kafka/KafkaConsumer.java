package com.example.kafka_performance_lab.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumer {

    @KafkaListener(topics = "order-create")
    public void listen(String message, Acknowledgment acknowledgment) throws InterruptedException {
        Thread.sleep(100);
        log.info("Processed {}", message);
        acknowledgment.acknowledge();
    }
}
