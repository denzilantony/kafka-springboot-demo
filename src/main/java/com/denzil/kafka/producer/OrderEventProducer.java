package com.denzil.kafka.producer;

import com.denzil.kafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${kafka.topic.order}")
    private String orderTopic;

    public CompletableFuture<SendResult<String, OrderEvent>>
            sendOrderEvent(OrderEvent event) {

        log.info("Sending order event: orderId={}, status={}",
                event.getOrderId(), event.getStatus());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(
                        orderTopic,
                        event.getOrderId(),
                        event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order event sent successfully: " +
                        "orderId={}, partition={}, offset={}",
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order event: " +
                        "orderId={}, error={}",
                        event.getOrderId(),
                        ex.getMessage());
            }
        });

        return future;
    }

    public CompletableFuture<SendResult<String, OrderEvent>>
            sendOrderEventToPartition(
                    OrderEvent event, int partition) {

        log.info("Sending order event to partition {}: orderId={}",
                partition, event.getOrderId());

        return kafkaTemplate.send(
                orderTopic,
                partition,
                event.getOrderId(),
                event);
    }
}