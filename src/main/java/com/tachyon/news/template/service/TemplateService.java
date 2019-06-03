package com.tachyon.news.template.service;

import com.tachyon.news.template.command.Command;
import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class TemplateService extends AbstractService {
    @Autowired
    private CommandFactory commandFactory;
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    public TemplateService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Message message)  {
        try {
            String routingKey = myContext.findRoutingKey(message);
            Command command = commandFactory.findCommand(routingKey);
            command.execute(message);
        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }


}
