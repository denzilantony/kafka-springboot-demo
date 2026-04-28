package com.denzil.kafka;

import com.denzil.kafka.controller.OrderController;
import com.denzil.kafka.model.OrderEvent;
import com.denzil.kafka.producer.OrderEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
@DisplayName("Order Controller Tests — TDD")
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private OrderEventProducer orderEventProducer;

    @Autowired
    private ObjectMapper objectMapper;

    private OrderEvent testEvent;

    @BeforeEach
    void setUp() {
        testEvent = OrderEvent.of(
                "ORDER-001",
                "CUSTOMER-001",
                "PRODUCT-001",
                2,
                new BigDecimal("59.98"));
    }

    @Test
    @DisplayName("POST /api/v1/orders - should accept order")
    void shouldAcceptOrder() throws Exception {
        when(orderEventProducer.sendOrderEvent(
                any(OrderEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        null));

        mockMvc.perform(post("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper
                        .writeValueAsString(testEvent)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.message")
                        .value("Order event published successfully"))
                .andExpect(jsonPath("$.orderId")
                        .value("ORDER-001"))
                .andExpect(jsonPath("$.status")
                        .value("ACCEPTED"));

        verify(orderEventProducer, times(1))
                .sendOrderEvent(any(OrderEvent.class));
    }

    @Test
    @DisplayName("POST /api/v1/orders/batch - should accept batch")
    void shouldAcceptBatchOrders() throws Exception {
        OrderEvent event2 = OrderEvent.of(
                "ORDER-002", "CUSTOMER-002",
                "PRODUCT-002", 1,
                new BigDecimal("29.99"));

        when(orderEventProducer.sendOrderEvent(
                any(OrderEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        null));

        mockMvc.perform(post("/api/v1/orders/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(
                        List.of(testEvent, event2))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.message")
                        .value("Batch order events published"))
                .andExpect(jsonPath("$.count").value("2"))
                .andExpect(jsonPath("$.status")
                        .value("ACCEPTED"));

        verify(orderEventProducer, times(2))
                .sendOrderEvent(any(OrderEvent.class));
    }

    @Test
    @DisplayName("POST /api/v1/orders - should reject invalid order")
    void shouldRejectInvalidOrder() throws Exception {
        OrderEvent invalidEvent = OrderEvent.builder()
                .orderId("")
                .customerId("")
                .productId("")
                .quantity(0)
                .totalAmount(new BigDecimal("-1"))
                .build();

        mockMvc.perform(post("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper
                        .writeValueAsString(invalidEvent)))
                .andExpect(status().isBadRequest());

        verify(orderEventProducer, never())
                .sendOrderEvent(any(OrderEvent.class));
    }
}