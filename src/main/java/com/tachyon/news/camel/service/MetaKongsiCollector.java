package com.tachyon.news.camel.service;

import com.tachyon.crawl.kind.KrxCrawler;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.camel.NewsConstants;
import com.tachyon.news.camel.config.MyContext;
import com.tachyon.news.camel.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 메타공시 수집..
 */
@Slf4j
@Service
public class MetaKongsiCollector extends DefaultService {
    @Autowired
    private MyContext myContext;
    @Autowired
    private CamelContext camelContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    public MetaKongsiCollector(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Exchange exchange) {
        try {
            infoView(exchange,log);
            String day = myContext.findInputValue(exchange);
            log.info("<<< " + day);
            log.info("TODO 메타공시 수집처리 "+day);
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);

        }
    }

}
