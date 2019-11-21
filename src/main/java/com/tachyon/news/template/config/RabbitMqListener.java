package com.tachyon.news.template.config;

import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.service.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.Map;

@Slf4j
@Component
public class RabbitMqListener {
    @Autowired
    private CommandFactory commandFactory;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @RabbitListener(queues = {"GATEWAY"})
    public void GATEWAY(Message message) {
        execute(message,"GATEWAY");
    }
    @RabbitListener(queues = {"OK_GATEWAY"})
    public void OK_GATEWAY(Message message) {
        execute(message,"OK_GATEWAY");
    }
    @RabbitListener(queues = {"KONGSI_CHECK"})
    public void KONGSI_CHECK(Message message) {
        execute(message,"KONGSI_CHECK");
    }
    @RabbitListener(queues = {"META_KONGSI"})
    public void META_KONGSI(Message message) {
        execute(message,"META_KONGSI");
    }


    @RabbitListener(queues = {"_STOCK_HOLDER","_ELASTICSEARCH_INDEX","_STAFF_HOLDER"
            ,"_PURPOSE_HOLDER","_KEYWORD_NOTIFICATION","_ACCESS_HOLDER","_TELEGRAM","_ROMOR_HOLDER","_LARGEST_SHARE_HOLDER"})
    public void TEMPLATE(Message message) {
        execute(message,"TEMPLATE");
    }
    @RabbitListener(queues = {"_RELATIVE_HOLDER","_STAFF_REPORT","_PROVISIONAL_PERF"})
    public void TEMPLATE2(Message message) {
        execute(message,"TEMPLATE");
    }

    @RabbitListener(queues = {"_SPLIT_QUEUE"})
    public void _SPLIT_QUEUE(Message message) {
        executeSplit(message);
    }

    private void execute(Message message, String queue) {
        if ("GATEWAY".equalsIgnoreCase(queue)) {
            NewsService newsService = commandFactory.findService(GatewayService.class);
            newsService.consume(message);
        } else if ("OK_GATEWAY".equalsIgnoreCase(queue)) {
            GatewayService newsService = (GatewayService)commandFactory.findService(GatewayService.class);
            newsService.okConsume(message);
        }else if ("KONGSI_CHECK".equalsIgnoreCase(queue)) {
            NewsService newsService = commandFactory.findService(KongsiCollector.class);
            newsService.consume(message);
        }else if ("META_KONGSI".equalsIgnoreCase(queue)) {
            NewsService newsService = commandFactory.findService(MetaKongsiCollector.class);
            newsService.consume(message);
        }else if ("TEMPLATE".equalsIgnoreCase(queue)) {
            NewsService newsService = commandFactory.findService(TemplateService.class);
            newsService.consume(message);
        }else {

        }
    }




    private void executeSplit(Message message) {
        byte[] bytes = message.getBody();
        String _message = new String(bytes, Charset.forName("UTF-8"));
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(_message, "\n");
        String queue = strings[0].trim();

        for (int i = 1; i < strings.length; i++) {
            Object value = strings[i].trim();
            if (StringUtils.isEmpty(value.toString())) {
                continue;
            }
            rabbitTemplate.convertAndSend(queue,value,convert(message.getMessageProperties().getHeaders()));
            log.info(queue+ " << "+value);
        }
    }

    private MessagePostProcessor convert(Map<String, Object> headers) {
        return new MessagePostProcessor(){
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                for (String key : headers.keySet()) {
                    message.getMessageProperties().getHeaders().put(key, headers.get(key));
                }
                return message;
            }
        };
    }
}
