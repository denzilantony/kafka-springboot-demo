package com.denzil.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 3,
    topics = {
        "order-events-test",
        "order-events-test.DLT",
        "notification-events-test"
    }
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=" +
        "${spring.embedded.kafka.brokers}",
    "kafka.topic.order=order-events-test",
    "kafka.topic.order.dlt=order-events-test.DLT",
    "kafka.topic.notification=notification-events-test"
})
class KafkaSpringbootDemoApplicationTests {

    @Test
    void contextLoads() {
    }
}