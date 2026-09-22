package com.denzil.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import com.denzil.kafka.model.OrderEvent;
import com.denzil.kafka.producer.OrderEventProducer;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(partitions = 3, topics = { "order-events-test", "order-events-test.DLT", "notification-events-test" })
@TestPropertySource(properties = { "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.producer.bootstrap-servers=" + "${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=" + "${spring.embedded.kafka.brokers}",
		"kafka.topic.order=order-events-test", "kafka.topic.order.dlt=order-events-test.DLT",
		"kafka.topic.notification=notification-events-test" })
@DisplayName("Order Event Producer Tests — TDD with Embedded Kafka")
class OrderEventProducerTest {

	@Autowired
	private OrderEventProducer orderEventProducer;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Value("${kafka.topic.order}")
	private String orderTopic;

	private KafkaMessageListenerContainer<String, OrderEvent> container;
	private BlockingQueue<ConsumerRecord<String, OrderEvent>> records;

	@BeforeEach
	void setUp() {
		Map<String, Object> consumerProperties = new HashMap<>(KafkaTestUtils
				.consumerProps("producer-test-group-" + System.currentTimeMillis(), "true", embeddedKafkaBroker));

		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		consumerProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.denzil.kafka.model");
		consumerProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
		consumerProperties.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		DefaultKafkaConsumerFactory<String, OrderEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(
				consumerProperties);

		ContainerProperties containerProperties = new ContainerProperties(orderTopic);

		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		records = new LinkedBlockingQueue<>();
		container.setupMessageListener((MessageListener<String, OrderEvent>) records::add);

		container.start();
		ContainerTestUtils.waitForAssignment(container, 3);
	}

	@AfterEach
	void tearDown() {
		container.stop();
		records.clear();
	}

	@Test
	@DisplayName("Should send order event to Kafka topic successfully")
	void shouldSendOrderEventSuccessfully() throws InterruptedException, ExecutionException, TimeoutException {

		String orderId = "ORDER-" + System.currentTimeMillis();

		OrderEvent event = OrderEvent.of(orderId, "CUSTOMER-001", "PRODUCT-001", 2, new BigDecimal("59.98"));

		orderEventProducer.sendOrderEvent(event);

		CompletableFuture<SendResult<String, OrderEvent>> future = orderEventProducer.sendOrderEvent(event);

		SendResult<String, OrderEvent> result = future.get(10, TimeUnit.SECONDS);

		assertThat(result).isNotNull();
		assertThat(result.getRecordMetadata().topic()).isEqualTo(orderTopic);
		assertThat(result.getRecordMetadata().partition()).isBetween(0, 2);
		assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);

		ConsumerRecord<String, OrderEvent> received = records.poll(10, TimeUnit.SECONDS);

		assertThat(received).isNotNull();
		assertThat(received.key()).isEqualTo(orderId);
		assertThat(received.value().getOrderId()).isEqualTo(orderId);
		assertThat(received.value().getCustomerId()).isEqualTo("CUSTOMER-001");
		assertThat(received.value().getQuantity()).isEqualTo(2);
		assertThat(received.value().getTotalAmount()).isEqualByComparingTo("59.98");
		assertThat(received.value().getStatus()).isEqualTo(OrderEvent.OrderStatus.CREATED);
	}

	@Test
	@DisplayName("Should send multiple order events successfully")
	void shouldSendMultipleOrderEvents() throws InterruptedException {

		String orderId1 = "ORDER-A-" + System.currentTimeMillis();
		String orderId2 = "ORDER-B-" + System.currentTimeMillis();

		OrderEvent event1 = OrderEvent.of(orderId1, "CUSTOMER-002", "PRODUCT-002", 1, new BigDecimal("29.99"));

		OrderEvent event2 = OrderEvent.of(orderId2, "CUSTOMER-003", "PRODUCT-003", 3, new BigDecimal("89.97"));

		orderEventProducer.sendOrderEvent(event1);
		orderEventProducer.sendOrderEvent(event2);

		ConsumerRecord<String, OrderEvent> received1 = records.poll(10, TimeUnit.SECONDS);
		ConsumerRecord<String, OrderEvent> received2 = records.poll(10, TimeUnit.SECONDS);

		assertThat(received1).isNotNull();
		assertThat(received2).isNotNull();

		java.util.List<String> receivedIds = java.util.Arrays.asList(received1.value().getOrderId(),
				received2.value().getOrderId());

		assertThat(receivedIds).contains(orderId1, orderId2);
	}

	@Test
	@DisplayName("Should use order id as message key")
	void shouldUseOrderIdAsMessageKey() throws InterruptedException {

		String orderId = "ORDER-KEY-" + System.currentTimeMillis();

		OrderEvent event = OrderEvent.of(orderId, "CUSTOMER-004", "PRODUCT-004", 1, new BigDecimal("19.99"));

		orderEventProducer.sendOrderEvent(event);

		ConsumerRecord<String, OrderEvent> received = records.poll(10, TimeUnit.SECONDS);

		assertThat(received).isNotNull();
		assertThat(received.key()).isEqualTo(orderId);
	}
}