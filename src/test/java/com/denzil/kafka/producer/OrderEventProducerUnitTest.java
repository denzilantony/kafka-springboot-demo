package com.denzil.kafka.producer;

import com.denzil.kafka.model.OrderEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventProducerUnitTest {

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private OrderEventProducer orderEventProducer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(
                orderEventProducer,
                "orderTopic",
                "order-events-test");
    }

    @Test
    void shouldPropagateKafkaSendFailure() {

        OrderEvent event = OrderEvent.of(
                "ORDER-FAIL-001",
                "CUSTOMER-001",
                "PRODUCT-001",
                1,
                new BigDecimal("19.99"));

        RuntimeException exception =
                new RuntimeException("Kafka unavailable");

        CompletableFuture<SendResult<String, OrderEvent>> failedFuture =
                new CompletableFuture<>();

        failedFuture.completeExceptionally(exception);

        when(kafkaTemplate.send(
                "order-events-test",
                event.getOrderId(),
                event))
                .thenReturn(failedFuture);

        CompletableFuture<SendResult<String, OrderEvent>> result =
                orderEventProducer.sendOrderEvent(event);

        assertThat(result).isCompletedExceptionally();

        verify(kafkaTemplate).send(
                "order-events-test",
                event.getOrderId(),
                event);
    }
}