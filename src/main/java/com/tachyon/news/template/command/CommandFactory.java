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
        } else if("_ROMOR_HOLDER".equalsIgnoreCase(routingKey)){
            return findCommand(RumorHolderCommand.class);
        } else if("_LARGEST_SHARE_HOLDER".equalsIgnoreCase(routingKey)){
            return findCommand(LargestShareHolderCommand.class);
        } else if("_RELATIVE_HOLDER".equalsIgnoreCase(routingKey)){
            return findCommand(RelativeHolderCommand.class);
        }else if("_STAFF_REPORT".equalsIgnoreCase(routingKey)){
            return findCommand(StaffReportCommand.class);
        }else if("_PROVISIONAL_PERF".equalsIgnoreCase(routingKey)){
            return findCommand(ProvisionalSalesPerformanceCommand.class);
        }else if("_TAKING".equalsIgnoreCase(routingKey)){
            return findCommand(TakingHolderCommand.class);
        }else if("_MY_STOCK".equalsIgnoreCase(routingKey)){
            return findCommand(MyStockCommand.class);
        }else if("_CONTRACT".equalsIgnoreCase(routingKey)){
            return findCommand(ContractCommand.class);
        }else if("_TOUCH".equalsIgnoreCase(routingKey)){
            return findCommand(TouchCommand.class);
        }else if("_TRIAL".equalsIgnoreCase(routingKey)){
            return findCommand(TrialHolderCommand.class);
        }else if("_OTHER_MANAGEMENT".equalsIgnoreCase(routingKey)){
            return findCommand(OtherManagementCommand.class);
        }else {
            return findCommand(DummyCommand.class);
        }
    }


}
