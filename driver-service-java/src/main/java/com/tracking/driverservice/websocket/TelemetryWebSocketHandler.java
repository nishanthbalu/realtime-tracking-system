

package com.tracking.driverservice.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracking.driverservice.config.KafkaProducerConfig;
import com.tracking.driverservice.model.DriverLocation;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class TelemetryWebSocketHandler extends TextWebSocketHandler {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper; // Spring's built-in JSON parser

    // Constructor injection (Best practice for Spring dependency injection)
    public TelemetryWebSocketHandler(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            String payload = message.getPayload();
            
            System.out.println("====Payload Received====:" + payload);
            
            // 1. Data Validation: Try deserializing the string into our Java object.
            // If it's missing fields or malformed, it throws an exception and gets caught.
            DriverLocation location = objectMapper.readValue(payload, DriverLocation.class);
            
            // 2. Chronological Ordering: Send payload to Kafka using driverId as the message KEY.
            kafkaTemplate.send(KafkaProducerConfig.TELEMETRY_TOPIC, location.getDriverId(), payload);
            
        } catch (Exception e) {
            // Logs bad payloads without crashing our persistent WebSocket stream connection
            System.err.println("Dropped malformed payload event: " + e.getMessage());
        }
    }
}