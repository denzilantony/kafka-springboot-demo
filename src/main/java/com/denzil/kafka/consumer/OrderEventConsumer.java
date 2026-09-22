package com.denzil.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import com.denzil.kafka.model.OrderEvent;
import com.denzil.kafka.service.OrderProcessingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventConsumer {
	
	private final OrderProcessingService orderProcessingService;

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

        orderProcessingService.processOrderEvent(event);
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

    private void handleFailedEvent(OrderEvent event) {
        log.error("Dead letter queue — " +
                "manually handle failed event: orderId={}",
                event.getOrderId());
    }
}