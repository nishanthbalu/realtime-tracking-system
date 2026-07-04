package com.tracking.driverservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.apachecommons.CommonsLog;

@Configuration
public class KafkaProducerConfig {
	
	// Centralizing the topic name to avoid typos across our classes
    public static final String TELEMETRY_TOPIC = "driver-telemetry";
    
    @Bean
    public NewTopic driverTelemetryTopic() {
    	
    	return TopicBuilder.name(TELEMETRY_TOPIC)
    			.partitions(3)
    			.replicas(1)
    			.build();
    }
    
 // Explicitly declaring the ObjectMapper bean to resolve the injection blocker
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

}
