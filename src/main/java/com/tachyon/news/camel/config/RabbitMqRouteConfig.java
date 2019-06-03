package com.tachyon.news.camel.config;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RabbitMqRouteConfig extends RouteBuilder {
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
    Queue KONGSI_CHECK2() {
        return QueueBuilder.durable("KONGSI_CHECK2").build();
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
        Queue queue =  QueueBuilder.durable("_ELASTICSEARCH_INDEX").build();
        log.info(queue.toString());
        return queue;
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
    @Override
    public void configure() throws Exception {
        // flow 큐
        // 기초공시먄 처리된 공시를 처음 받는 큐.
        configureRabbitMq("GATEWAY", "bean:gatewayService?method=consume");
        // 공시파일을 처리한 후에 가는 큐..
        configureRabbitMq("OK_GATEWAY", "bean:gatewayService?method=okConsume");



        // 공시파일을 처리하는 큐1..
        configureRabbitMq("KONGSI_CHECK", "bean:kongsiCollector?method=consume");
        // 공시파일을 처리하는 큐2..
        configureRabbitMq("KONGSI_CHECK2", "bean:kongsiCollector?method=consume");


        // 메타공시를 처리하는 큐..
        configureRabbitMq("META_KONGSI", "bean:metaKongsiCollector?method=consume");
        // 템플릿큐
        configureRabbitMq("_STOCK_HOLDER", "bean:templateService?method=consume");
        configureRabbitMq("_ELASTICSEARCH_INDEX", "bean:templateService?method=consume");
        configureRabbitMq("_STAFF_HOLDER", "bean:templateService?method=consume");
        configureRabbitMq("_PURPOSE_HOLDER", "bean:templateService?method=consume");
        // 웹검색큐
        configureRabbitMq("_ACCESS_HOLDER", "bean:templateService?method=consume");
        // SPLIT큐
        configureRabbitMq("_SPLIT_QUEUE", "bean:toolsService?method=split");

        // 에러큐..
        configureRabbitMq("_ERROR_QUEUE", "bean:toolsService?method=error");
    }

    private void configureRabbitMq(String from,String to) {
        from(findRabbitMqFrom(from)).id("from" + from)
                .to(to);
    }

    private String findRabbitMqFrom(String queueName) {
        return myContext.findFromRabbitMq(queueName);
    }

}
