package com.tachyon.news.template.config;

import com.tachyon.news.template.command.CommandFactory;
import com.tachyon.news.template.service.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
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

    @RabbitListener(queues = {"GATEWAY"}, concurrency = "1")
    public void GATEWAY(Message message) {
        commandFactory.findService(GatewayService.class).consume(message);
    }

    @RabbitListener(queues = {"OK_GATEWAY"}, concurrency = "1")
    public void OK_GATEWAY(Message message) {
        GatewayService newsService = (GatewayService)commandFactory.findService(GatewayService.class);
        newsService.okConsume(message);
    }

    @RabbitListener(queues = {"KONGSI_CHECK"}, concurrency = "1")
    public void KONGSI_CHECK(Message message) {
        commandFactory.findService(KongsiCollector.class).consume(message);
    }

    @RabbitListener(queues = {"META_KONGSI"}, concurrency = "1")
    public void META_KONGSI(Message message) {
        commandFactory.findService(MetaKongsiCollector.class).consume(message);
    }

    @RabbitListener(queues = {"LOCAL_KONGSI"}, concurrency = "1")
    public void LOCAL_KONGSI(Message message) {
        commandFactory.findService(LocalKongsiService.class).consume(message);
    }

    @RabbitListener(queues = {"_STOCK_HOLDER"}, concurrency = "1")
    public void _STOCK_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_ELASTICSEARCH_INDEX"}, concurrency = "1")
    public void _ELASTICSEARCH_INDEX(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_STAFF_HOLDER"}, concurrency = "1")
    public void _STAFF_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_PURPOSE_HOLDER"}, concurrency = "1")
    public void _PURPOSE_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_KEYWORD_NOTIFICATION"}, concurrency = "1")
    public void _KEYWORD_NOTIFICATION(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_ACCESS_HOLDER"}, concurrency = "1")
    public void _ACCESS_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_TELEGRAM"}, concurrency = "1")
    public void _TELEGRAM(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_ROMOR_HOLDER"}, concurrency = "1")
    public void _ROMOR_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_LARGEST_SHARE_HOLDER"}, concurrency = "1")
    public void _LARGEST_SHARE_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_RELATIVE_HOLDER"}, concurrency = "1")
    public void _RELATIVE_HOLDER(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_STAFF_REPORT"}, concurrency = "1")
    public void _STAFF_REPORT(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_PROVISIONAL_PERF"}, concurrency = "1")
    public void _PROVISIONAL_PERF(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_TAKING"}, concurrency = "1")
    public void _TAKING(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_MY_STOCK"}, concurrency = "1")
    public void _MY_STOCK(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_CONTRACT"}, concurrency = "1")
    public void _CONTRACT(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_TOUCH"}, concurrency = "1")
    public void _TOUCH(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_TRIAL"}, concurrency = "1")
    public void _TRIAL(Message message) {
        commandFactory.findService(TemplateService.class).consume(message);
    }

    @RabbitListener(queues = {"_SPLIT_QUEUE"}, concurrency = "1")
    public void _SPLIT_QUEUE(Message message) {
        executeSplit(message);
    }



//    @RabbitListener(queues = {"GATEWAY","OK_GATEWAY","KONGSI_CHECK","META_KONGSI","LOCAL_KONGSI"})
//    public void SERVICE(Message message) {
//        execute(message,message.getMessageProperties().getConsumerQueue());
//    }

//    @RabbitListener(queues = {"_STOCK_HOLDER","_ELASTICSEARCH_INDEX","_STAFF_HOLDER"
//            ,"_PURPOSE_HOLDER","_KEYWORD_NOTIFICATION","_ACCESS_HOLDER","_TELEGRAM","_ROMOR_HOLDER","_LARGEST_SHARE_HOLDER"})
//    public void TEMPLATE(Message message) {
//        execute(message,"TEMPLATE");
//    }
//    @RabbitListener(queues = {"_RELATIVE_HOLDER","_STAFF_REPORT","_PROVISIONAL_PERF","_TAKING","_MY_STOCK","_CONTRACT","_TOUCH","_TRIAL"})
//    public void TEMPLATE2(Message message) {
//        execute(message,"TEMPLATE");
//    }

//    @RabbitListener(queues = {"_SPLIT_QUEUE"})
//    public void _SPLIT_QUEUE(Message message) {
//        executeSplit(message);
//    }

//    private void execute(Message message, String queue) {
//        if ("GATEWAY".equalsIgnoreCase(queue)) {
//            NewsService newsService = commandFactory.findService(GatewayService.class);
//            newsService.consume(message);
//        } else if ("OK_GATEWAY".equalsIgnoreCase(queue)) {
//            GatewayService newsService = (GatewayService)commandFactory.findService(GatewayService.class);
//            newsService.okConsume(message);
//        }else if ("KONGSI_CHECK".equalsIgnoreCase(queue)) {
//            NewsService newsService = commandFactory.findService(KongsiCollector.class);
//            newsService.consume(message);
//        }else if ("META_KONGSI".equalsIgnoreCase(queue)) {
//            NewsService newsService = commandFactory.findService(MetaKongsiCollector.class);
//            newsService.consume(message);
//        }else if ("TEMPLATE".equalsIgnoreCase(queue)) {
//            commandFactory.findService(TemplateService.class).consume(message);
//        } else if ("LOCAL_KONGSI".equalsIgnoreCase(queue)) {
//            NewsService newsService = commandFactory.findService(LocalKongsiService.class);
//            newsService.consume(message);
////        }else if("BEFORE_KONGSI".equalsIgnoreCase(queue)){
////            NewsService newsService = commandFactory.findService(BeforeKongsiService.class);
////            newsService.consume(message);
//        } else {
//
//        }
//    }




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
