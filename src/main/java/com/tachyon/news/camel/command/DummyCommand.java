package com.tachyon.news.camel.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DummyCommand implements Command {
    @Override
    public void execute(Exchange key) throws Exception {

        log.info("<<< "+key);
    }
}
