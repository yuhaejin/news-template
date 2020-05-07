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
    Queue _RELATIVE_HOLDER() {
        return QueueBuilder.durable("_RELATIVE_HOLDER").build();
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

    @Bean
    Queue _ROMOR_HOLDER() {
        return QueueBuilder.durable("_ROMOR_HOLDER").build();
    }


    @Bean
    Queue _LARGEST_SHARE_HOLDER() {
        return QueueBuilder.durable("_LARGEST_SHARE_HOLDER").build();
    }

    @Bean
    Queue _STAFF_REPORT() {
        return QueueBuilder.durable("_STAFF_REPORT").build();
    }

    /**
     * 잠정적인 영업실적
     *
     * @return
     */
    @Bean
    Queue _PROVISIONAL_PERF() {
        return QueueBuilder.durable("_PROVISIONAL_PERF").build();
    }

    @Bean
    Queue LOCAL_KONGSI() {
        return QueueBuilder.durable("LOCAL_KONGSI").build();
    }

    //_TAKING
    @Bean
    Queue _TAKING() {
        return QueueBuilder.durable("_TAKING").build();
    }

//    @Bean
//    Queue BEFORE_KONGSI() {
//        return QueueBuilder.durable("BEFORE_KONGSI").build();
//    }

    @Bean
    Queue _MY_STOCK() {
        return QueueBuilder.durable("_MY_STOCK").build();
    }

    @Bean
    Queue _CONTRACT() {
        return QueueBuilder.durable("_CONTRACT").build();
    }

    @Bean
    Queue _TOUCH() {
        return QueueBuilder.durable("_TOUCH").build();
    }
    @Bean
    Queue _TRIAL() {
        return QueueBuilder.durable("_TRIAL").build();
    }


}
