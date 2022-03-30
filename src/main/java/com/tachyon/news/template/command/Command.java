package com.tachyon.news.template.command;

import org.springframework.amqp.core.Message;

public interface Command {
    void execute(Message message) throws Exception;

    String findArticleType();
}
