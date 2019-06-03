package com.tachyon.news.camel.command;

import org.apache.camel.Exchange;

public interface Command {
    void execute(Exchange exchange) throws Exception;
}
