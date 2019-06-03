package com.tachyon.news.camel.service;

import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.camel.NewsConstants;
import com.tachyon.news.camel.config.MyContext;
import com.tachyon.news.camel.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class GatewayService extends DefaultService {
    @Autowired
    private MyContext myContext;
    @Autowired
    private CamelContext camelContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    public GatewayService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    /**
     * 맨 처음 받을 때
     * @param exchange
     */
    public void consume(Exchange exchange) {
        try {
            handle(exchange, false);
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }
    }

    /**
     * 공시파일을 처리하고 난 다음에 받을 때...
     * @param exchange
     */
    public void okConsume(Exchange exchange) {
        try {
            handle(exchange, true);
        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }
    }




    private void handle(Exchange exchange,boolean fromKongsiCollector) {
        infoView(exchange,log);
        String key = myContext.findInputValue(exchange);
        log.info("<<< " + key);

        if (StringUtils.isEmpty(key)) {
            log.error("invalid input ....");
            return;
        }
        String[] keys = StringUtils.split(key, "_");
        String docNo = keys[0];
        String code = keys[1];
        String acptNo = keys[2];
        Map<String, Object> kongsi = null;

        if (fromKongsiCollector) {
            kongsi = myContext.findKongsiMap(exchange);
            requestTemplate(key,kongsi,exchange);
        } else {
            kongsi = findKongsiHalder2(docNo, code, acptNo);
            log.info("kongsiHolder " + kongsi);
            if (kongsi == null) {
                log.error("기초공시가 없음.. " + key);
                return;
            }
            String docUrl = Maps.getValue(kongsi, "doc_url");
            if (StringUtils.isEmpty(docUrl)) {
                log.error("공시URL이 없음.. " + key);
                return;
            }

            myContext.addGatewayCalling();
            checkMetaKongsi(acptNo,code);
            checkKongsiFile2(key, acptNo, code, docNo, kongsi);
        }
    }

    private void checkKongsiFile2(String key, String acptNo, String code, String docNo, Map<String, Object> kongsi) {
        String path = fileHtmlPath(myContext.getHtmlTargetPath(), docNo, code);
        Map<String, Object> headers = headers(kongsi, path);
        requestCollectMetaKongsi(key, headers);
    }

    private String now() {
        Date date = new Date();
        return DateUtils.dayString(date, "yyyyMMdd");
    }

    private void checkMetaKongsi(String acptNo, String code) {
        String day = acptNo.substring(0, 8);
        if (now().equalsIgnoreCase(day)) {
            log.info("오늘 메타공시 체크는 SKIP " + day);
            return;
        }
        File f = findJsonBasicKongsiPath(myContext.getJsonTargetPath(), acptNo, code);
        boolean metaKongsiOk = false;
        if (f == null) {
            log.error("메타공시 파일이 없음. " + day);
            metaKongsiOk = false;
        } else {
            if (f.exists() == false) {
                log.error("메타공시가 파일이 이상함. " + f.getAbsolutePath());
                metaKongsiOk = false;
            } else {
                metaKongsiOk = true;
            }
        }

        if (metaKongsiOk == false) {
            // 수집요청..
            log.error("메타공시 수집 요청 " + day);
            Map<String, Object> headers = createHeaders();
//            requestBodyAndHeaders(camelContext, myContext.findFromRabbitMq("META_KONGSI"), day, headers);
            requestBodyAndHeaders2(rabbitTemplate, "META_KONGSI", day,headers);

        }
    }


    private Map<String, Object> createHeaders() {
        return new HashMap<>();
    }

    private boolean fromKongsiCollect(Exchange exchange) {
        Map<String, Object> headers = exchange.getIn().getHeaders();
        if (headers.containsKey("KONGSI_CHECK")) {
            return true;
        } else {
            return false;
        }
    }

    private boolean checkKongsiFile(String key, String acptNo, String code, String docNo, Map<String, Object> kongsi) {

        String path = fileHtmlPath(myContext.getHtmlTargetPath(), docNo, code);

        File ff = new File(path);
        boolean kongsiFileOk = false;
        if (ff == null) {
            log.error("공시파일이 없음. " + key);
            kongsiFileOk = false;
        } else {
            if (ff.exists() == false) {
                // 이런 경우는 없음...
                log.error("공시파일 이상함. " + ff.getAbsolutePath());
                kongsiFileOk = false;
            } else {
                kongsiFileOk = true;
            }
        }

        if (kongsiFileOk == false) {
            Map<String, Object> headers = headers(kongsi, path);
            requestCollectMetaKongsi(key, headers);
        } else {
            log.info("공시파일 모두 존재.. " + key);
        }

        return kongsiFileOk;
    }

    private void requestTemplate(String key, Map<String, Object> kongsi, Exchange exchange) {
        String[] templates = myContext.getTemplates();
        Map<String, Object> headers = new HashMap<>();
        setupPreviousHeaders(exchange,headers);
        setupKongsiMap(headers, kongsi);
        for (String template : templates) {
            // TemplateService에서 분기함..
            requestBodyAndHeaders2(rabbitTemplate, template, key,headers);
//            requestBodyAndHeaders(camelContext, myContext.findFromRabbitMq(template), key, headers);
        }
    }

    private void requestCollectMetaKongsi(String key, Map<String, Object> headers) {
//        requestBodyAndHeaders(camelContext, myContext.findFromRabbitMq("KONGSI_CHECK"), key, headers);
        long l = myContext.getGatewayCalling();
        if (l % 2 == 0) {
            requestBodyAndHeaders2(rabbitTemplate,"KONGSI_CHECK2", key,headers);
        } else {
            requestBodyAndHeaders2(rabbitTemplate,"KONGSI_CHECK", key,headers);
        }
    }


    private Map<String, Object> headers(Map<String, Object> kongsi, String path) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(NewsConstants.KONGSI_FILE_PATH, path);
        setupKongsiMap(headers, kongsi);
        return headers;
    }


}
