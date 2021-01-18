package com.tachyon.news.template.command;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DummyCommand implements Command {
    @Override
    public void execute(Message message) throws Exception {

        log.info("<<< "+message);
    }

    @Override
    public String findArticleType() {
        return "";
    }
}
