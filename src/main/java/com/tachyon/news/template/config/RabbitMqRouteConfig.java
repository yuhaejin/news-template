package com.tachyon.news.template.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RabbitMqRouteConfig {
    @Autowired
    private MyContext myContext;

    @Bean
    Queue GATEWAY() {
        return QueueBuilder.durable("GATEWAY").build();
    }

    @Bean
    Queue OK_GATEWAY() {
        return QueueBuilder.durable("OK_GATEWAY").build();
    }

    @Bean
    Queue KONGSI_CHECK() {
        return QueueBuilder.durable("KONGSI_CHECK").build();
    }

    @Bean
    Queue META_KONGSI() {
        return QueueBuilder.durable("META_KONGSI").build();
    }

    @Bean
    Queue _STOCK_HOLDER() {
        return QueueBuilder.durable("_STOCK_HOLDER").build();
    }

    @Bean
    Queue _ELASTICSEARCH_INDEX() {
        Queue queue = QueueBuilder.durable("_ELASTICSEARCH_INDEX").build();
        return queue;
    }

    @Bean
    Queue _KEYWORD_NOTIFICATION() {
        return QueueBuilder.durable("_KEYWORD_NOTIFICATION").build();
    }

    @Bean
    Queue _STAFF_HOLDER() {
        return QueueBuilder.durable("_STAFF_HOLDER").build();
    }

    @Bean
    Queue _PURPOSE_HOLDER() {
        return QueueBuilder.durable("_PURPOSE_HOLDER").build();
    }

    @Bean
    Queue _ACCESS_HOLDER() {
        return QueueBuilder.durable("_ACCESS_HOLDER").build();
    }

    @Bean
    Queue _ERROR_QUEUE() {
        return QueueBuilder.durable("_ERROR_QUEUE").build();
    }
    @Bean
    Queue _TELEGRAM() {
        return QueueBuilder.durable("_TELEGRAM").build();
    }

    private String findRabbitMqFrom(String queueName) {
        return myContext.findFromRabbitMq(queueName);
    }

}
