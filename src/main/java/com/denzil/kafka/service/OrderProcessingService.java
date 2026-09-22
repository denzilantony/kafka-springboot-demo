package com.denzil.kafka.service;

import com.denzil.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderProcessingService {

    public void processOrderEvent(OrderEvent event) {

        log.info("Processing order event: orderId={}",
                event.getOrderId());

        switch (event.getStatus()) {
            case CREATED -> handleCreatedOrder(event);
            case PROCESSING -> handleProcessingOrder(event);
            case COMPLETED -> handleCompletedOrder(event);
            case FAILED -> handleFailedOrder(event);
            case CANCELLED -> handleCancelledOrder(event);
            default -> log.warn("Unknown order status: {}",
                    event.getStatus());
        }
    }

    private void handleCreatedOrder(OrderEvent event) {
        log.info("New order created: orderId={}, " +
                "customerId={}, amount={}",
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotalAmount());
    }

    private void handleProcessingOrder(OrderEvent event) {
        log.info("Order being processed: orderId={}",
                event.getOrderId());
    }

    private void handleCompletedOrder(OrderEvent event) {
        log.info("Order completed successfully: orderId={}",
                event.getOrderId());
    }

    private void handleFailedOrder(OrderEvent event) {
        log.error("Order failed: orderId={}, reason={}",
                event.getOrderId(),
                event.getErrorMessage());
    }

    private void handleCancelledOrder(OrderEvent event) {
        log.info("Order cancelled: orderId={}",
                event.getOrderId());
    }
}