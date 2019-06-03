package com.tachyon.news.camel.service;

import com.tachyon.crawl.BizUtils;
import com.tachyon.crawl.kind.KrxCrawler;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.util.JSoupHelper;
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
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * 공시 파일과 텍스트 파일 처리
 */
@Slf4j
@Service
public class ToolsService extends DefaultService {
    @Autowired
    private MyContext myContext;
    @Autowired
    private CamelContext camelContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Autowired
    private RabbitTemplate rabbitTemplate;


    public ToolsService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    public void consume(Exchange exchange) {
        try {
            infoView(exchange,log);
            String key = myContext.findInputValue(exchange);
            log.info("<<< " + key);
            String[] keys = StringUtils.split(key, "_");
            String docNo = keys[0];
            String code = keys[1];
            String acptNo = keys[2];
            Map<String,Object> kongsiMap = findKongsiHalder2(docNo, code, acptNo);

            if (kongsiMap != null) {
                String docUrl = Maps.getValue(kongsiMap, "doc_url");
                String content = loadBalancerCommandHelper.findBody(docUrl);
                log.info(content.length()+" <<< "+StringUtils.abbreviate(content, 100));

            }
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }
    }

    public void split(Exchange exchange) {
        try {
            String key = myContext.findInputValue(exchange);
            String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(key, "\n");
            String queue = strings[0].trim();

            for (int i = 1; i < strings.length; i++) {
                String value = strings[i].trim();

                rabbitTemplate.convertAndSend(queue,value);
                log.info(queue+ " << "+value);
            }
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }

    }

    public void error(Exchange exchange) {

        String value = findKey(exchange);
        String fromQueue = findHeader(exchange,NewsConstants.FROM_QUEUE);
        String errmsg = findHeader(exchange,NewsConstants.ERR_MSG);

        log.info(fromQueue+" ===> "+ value+" "+errmsg);

        saveError(fromQueue, value,errmsg);
    }


}
