package com.denzil.kafka;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.denzil.kafka.consumer.OrderEventConsumer;
import com.denzil.kafka.model.OrderEvent;
import com.denzil.kafka.service.OrderProcessingService;

@ExtendWith(MockitoExtension.class)
@DisplayName("Order Event Consumer Tests — TDD")
class OrderEventConsumerTest {
	
	@Mock
	private OrderProcessingService orderProcessingService;

    @InjectMocks
    private OrderEventConsumer orderEventConsumer;

    @Test
    @DisplayName("Should process CREATED order event without error")
    void shouldProcessCreatedOrderEvent() {
        OrderEvent event = OrderEvent.of(
                "ORDER-001", "CUSTOMER-001",
                "PRODUCT-001", 1,
                new BigDecimal("29.99"));

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEvent(
                        event,
                        "order-events",
                        0,
                        0L));
        verify(orderProcessingService).processOrderEvent(event);
    }

    @Test
    @DisplayName("Should process PROCESSING order event without error")
    void shouldProcessProcessingOrderEvent() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-002")
                .customerId("CUSTOMER-002")
                .productId("PRODUCT-002")
                .quantity(2)
                .totalAmount(new BigDecimal("59.98"))
                .status(OrderEvent.OrderStatus.PROCESSING)
                .build();

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEvent(
                        event,
                        "order-events",
                        0,
                        1L));
        verify(orderProcessingService).processOrderEvent(event);
    }

    @Test
    @DisplayName("Should process COMPLETED order event without error")
    void shouldProcessCompletedOrderEvent() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-003")
                .customerId("CUSTOMER-003")
                .productId("PRODUCT-003")
                .quantity(1)
                .totalAmount(new BigDecimal("19.99"))
                .status(OrderEvent.OrderStatus.COMPLETED)
                .build();

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEvent(
                        event,
                        "order-events",
                        0,
                        2L));
        verify(orderProcessingService).processOrderEvent(event);
    }

    @Test
    @DisplayName("Should process FAILED order event without error")
    void shouldProcessFailedOrderEvent() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-004")
                .customerId("CUSTOMER-004")
                .productId("PRODUCT-004")
                .quantity(1)
                .totalAmount(new BigDecimal("49.99"))
                .status(OrderEvent.OrderStatus.FAILED)
                .errorMessage("Payment declined")
                .build();

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEvent(
                        event,
                        "order-events",
                        0,
                        3L));
        verify(orderProcessingService).processOrderEvent(event);
    }

    @Test
    @DisplayName("Should process CANCELLED order event without error")
    void shouldProcessCancelledOrderEvent() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-005")
                .customerId("CUSTOMER-005")
                .productId("PRODUCT-005")
                .quantity(1)
                .totalAmount(new BigDecimal("39.99"))
                .status(OrderEvent.OrderStatus.CANCELLED)
                .build();

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEvent(
                        event,
                        "order-events",
                        0,
                        4L));
        verify(orderProcessingService).processOrderEvent(event);
    }

    @Test
    @DisplayName("Should handle DLT event without error")
    void shouldHandleDltEvent() {
        OrderEvent event = OrderEvent.builder()
                .orderId("ORDER-006")
                .customerId("CUSTOMER-006")
                .productId("PRODUCT-006")
                .quantity(1)
                .totalAmount(new BigDecimal("99.99"))
                .status(OrderEvent.OrderStatus.FAILED)
                .errorMessage("Max retries exceeded")
                .build();

        assertThatNoException().isThrownBy(() ->
                orderEventConsumer.consumeOrderEventDlt(
                        event,
                        "order-events.DLT"));
    }
}