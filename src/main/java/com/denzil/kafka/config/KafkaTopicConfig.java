package com.denzil.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.order}")
    private String orderTopic;

    @Value("${kafka.topic.order.dlt}")
    private String orderDltTopic;

    @Value("${kafka.topic.notification}")
    private String notificationTopic;

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder
                .name(orderTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder
                .name(orderDltTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationEventsTopic() {
        return TopicBuilder
                .name(notificationTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}