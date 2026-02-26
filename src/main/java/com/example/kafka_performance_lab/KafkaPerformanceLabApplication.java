package com.example.kafka_performance_lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaPerformanceLabApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaPerformanceLabApplication.class, args);
	}

}
