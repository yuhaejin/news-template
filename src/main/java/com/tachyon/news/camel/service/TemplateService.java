package com.tachyon.news.camel.service;

import com.tachyon.news.camel.command.Command;
import com.tachyon.news.camel.command.CommandFactory;
import com.tachyon.news.camel.config.MyContext;
import com.tachyon.news.camel.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TemplateService extends DefaultService {
    @Autowired
    private CommandFactory commandFactory;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    public TemplateService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Exchange exchange) throws Exception {
        try {
            String routingKey = myContext.findRoutingKey(exchange);
            Command command = commandFactory.findCommand(routingKey);
            command.execute(exchange);
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }
    }


}
