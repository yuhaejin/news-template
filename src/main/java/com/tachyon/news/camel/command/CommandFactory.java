package com.tachyon.news.camel.command;

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
        } else {
            return findCommand(DummyCommand.class);
        }
    }


}
