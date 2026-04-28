package com.denzil.kafka.model;

import jakarta.validation.constraints.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    private String eventId;

    @NotBlank(message = "Order ID cannot be blank")
    private String orderId;

    @NotBlank(message = "Customer ID cannot be blank")
    private String customerId;

    @NotBlank(message = "Product ID cannot be blank")
    private String productId;

    @NotNull(message = "Quantity cannot be null")
    @Min(value = 1, message = "Quantity must be at least 1")
    private Integer quantity;

    @NotNull(message = "Total amount cannot be null")
    @DecimalMin(value = "0.0", inclusive = false,
                message = "Total amount must be greater than 0")
    private BigDecimal totalAmount;

    @NotNull(message = "Status cannot be null")
    private OrderStatus status;

    private LocalDateTime createdAt;
    private String errorMessage;

    public enum OrderStatus {
        CREATED, PROCESSING, COMPLETED, FAILED, CANCELLED
    }

    public static OrderEvent of(String orderId,
                                 String customerId,
                                 String productId,
                                 Integer quantity,
                                 BigDecimal totalAmount) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(orderId)
                .customerId(customerId)
                .productId(productId)
                .quantity(quantity)
                .totalAmount(totalAmount)
                .status(OrderStatus.CREATED)
                .createdAt(LocalDateTime.now())
                .build();
    }
}