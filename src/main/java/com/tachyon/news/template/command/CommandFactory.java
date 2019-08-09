package com.tachyon.news.template.command;

import com.tachyon.news.template.service.NewsService;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class CommandFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private Command findCommand(Class<? extends Command> commandClass) {
        return applicationContext.getBean(commandClass);
    }
    public NewsService findService(Class<? extends NewsService> commandClass) {
        return applicationContext.getBean(commandClass);
    }

    public Object findBean(Class aClass) {
        return applicationContext.getBean(aClass);
    }
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Command findCommand(String routingKey) {
        if ("_STOCK_HOLDER".equalsIgnoreCase(routingKey)) {
            return findCommand(StockHolderCommand.class);
        } else if ("_ELASTICSEARCH_INDEX".equalsIgnoreCase(routingKey)) {
            return findCommand(IndexingCommand.class);
        } else if ("_STAFF_HOLDER".equalsIgnoreCase(routingKey)) {
            return findCommand(StaffHolderCommand.class);
        } else if ("_PURPOSE_HOLDER" .equalsIgnoreCase(routingKey)) {
            return findCommand(PurposeHolderCommand.class);
        } else if ("_ACCESS_HOLDER".equalsIgnoreCase(routingKey)) {
            return findCommand(AccessHolderCommand.class);
        }else if ("_KEYWORD_NOTIFICATION".equalsIgnoreCase(routingKey)) {
            return findCommand(KeywordKongsiCollectorCommand.class);
//        } else if ("_TELEGRAM".equalsIgnoreCase(routingKey)) {
//            return findCommand(TelegramCommand.class);
        } else if("_ROMOR_HOLDER".equalsIgnoreCase(routingKey)){
            return findCommand(RumorHolderCommand.class);
        } else if("_LARGEST_SHARE_HOLDER".equalsIgnoreCase(routingKey)){
            return findCommand(LargestShareHolderCommand.class);
        } else {
            return findCommand(DummyCommand.class);
        }
    }


}
