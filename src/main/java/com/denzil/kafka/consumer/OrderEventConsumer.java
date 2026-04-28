package com.denzil.kafka.consumer;

import com.denzil.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderEventConsumer {

    @RetryableTopic(
        attempts = "3",
        backoff = @Backoff(delay = 1000, multiplier = 2.0),
        topicSuffixingStrategy =
            TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltTopicSuffix = ".DLT"
    )
    @KafkaListener(
        topics = "${kafka.topic.order}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderEvent(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION)
                    int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received order event: " +
                "orderId={}, status={}, " +
                "topic={}, partition={}, offset={}",
                event.getOrderId(),
                event.getStatus(),
                topic, partition, offset);

        processOrderEvent(event);
    }

    @KafkaListener(
        topics = "${kafka.topic.order.dlt}",
        groupId = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void consumeOrderEventDlt(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.error("Processing failed order event from DLT: " +
                "orderId={}, topic={}",
                event.getOrderId(), topic);

        handleFailedEvent(event);
    }

    private void processOrderEvent(OrderEvent event) {
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

    private void handleFailedEvent(OrderEvent event) {
        log.error("Dead letter queue — " +
                "manually handle failed event: orderId={}",
                event.getOrderId());
    }
}