package com.denzil.kafka.controller;

import com.denzil.kafka.model.OrderEvent;
import com.denzil.kafka.producer.OrderEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    @PostMapping
    public ResponseEntity<Map<String, String>> placeOrder(
            @Valid @RequestBody OrderEvent orderEvent) {

        log.info("Received order request: orderId={}",
                orderEvent.getOrderId());

        orderEventProducer.sendOrderEvent(orderEvent);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(Map.of(
                    "message", "Order event published successfully",
                    "orderId", orderEvent.getOrderId(),
                    "status", "ACCEPTED"
                ));
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, String>> placeBatchOrders(
            @Valid @RequestBody java.util.List<OrderEvent> events) {

        log.info("Received batch order request: count={}",
                events.size());

        events.forEach(orderEventProducer::sendOrderEvent);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(Map.of(
                    "message", "Batch order events published",
                    "count", String.valueOf(events.size()),
                    "status", "ACCEPTED"
                ));
    }
}