package com.tachyon.news.template.service;

import org.springframework.amqp.core.Message;

public interface NewsService {
    public void consume(Message message);
}
