package com.tachyon.news.template.service;

import com.tachyon.crawl.kind.model.DocNoIsuCd;
import com.tachyon.crawl.kind.model.Kongsi;
import com.tachyon.crawl.kind.util.DateUtils;
import com.tachyon.crawl.kind.util.JsonUtils;
import com.tachyon.crawl.kind.util.Maps;
import com.tachyon.news.template.NewsConstants;
import com.tachyon.news.template.config.MyContext;
import com.tachyon.news.template.repository.TemplateMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class GatewayService extends AbstractService {
    @Autowired
    private MyContext myContext;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    public GatewayService(TemplateMapper templateMapper) {
        super(templateMapper);
    }

    /**
     * 맨 처음 받을 때
     * @param message
     */
    public void consume(Message message) {
        try {
            handle(message, false);
        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }

    /**
     * 공시파일을 처리하고 난 다음에 받을 때...
     * @param message
     */
    public void okConsume(Message message) {
        try {
            handle(message, true);
        } catch (Exception e) {
            handleError(rabbitTemplate, message, e, log);
        }
    }


    private void handle(Message message,boolean fromKongsiCollector) throws IOException {
        infoView(message,log);
        String key = myContext.findInputValue(message);
        log.info("<<< " + key);

        if (StringUtils.isEmpty(key)) {
            log.error("invalid input ....");
            return;
        }
        DocNoIsuCd docNoIsuCd = findDocNoIsuCd(key);
        String docNo = docNoIsuCd.getDocNo();
        String code = docNoIsuCd.getIsuCd();
        String acptNo = docNoIsuCd.getAcptNo();
        Map<String, Object> kongsi = null;

        if (fromKongsiCollector) {
            kongsi = myContext.findKongsiMap(message);
            requestTemplate(key,kongsi,message);
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
            checkMetaKongsi(key,acptNo,code);
            checkKongsiFile2(key, acptNo, code, docNo, kongsi,message);
        }
    }

    private void checkKongsiFile2(String key, String acptNo, String code, String docNo, Map<String, Object> kongsi,Message message) {
        String path = fileHtmlPath(myContext.getHtmlTargetPath(), docNo, code);
        Map<String, Object> headers = headers(kongsi, path);
        setupPreviousHeaders(message,headers);
        requestCollectMetaKongsi(key, headers);
    }

    private String now() {
        Date date = new Date();
        return DateUtils.dayString(date, "yyyyMMdd");
    }

    private void checkMetaKongsi(String key,String acptNo, String code) {
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
            try {
                if (f.exists() == false) {
                    log.error("메타공시가 파일이 이상함. " + f.getAbsolutePath());
                    metaKongsiOk = false;
                } else {
                    String json = FileUtils.readFileToString(f, "UTF-8");
                    Kongsi kongsi = JsonUtils.toObject(json, Kongsi.class);
                    if (kongsi.getUrlInfos().size() == 0) {
                        metaKongsiOk = false;
                    } else {
                        metaKongsiOk = true;
                    }
                }

            } catch (Exception e) {
                // 예외발생하면 재수집하게 함.
                metaKongsiOk = false;
            }

        }

        if (metaKongsiOk == false) {
            // 수집요청..
            log.error("메타공시 수집 요청 " + key);
            Map<String, Object> headers = createHeaders();
            requestBodyAndHeaders2(rabbitTemplate, "META_KONGSI", key,headers);

        }
    }


    private Map<String, Object> createHeaders() {
        return new HashMap<>();
    }

    private void requestTemplate(String key, Map<String, Object> kongsi, Message message) {
        String[] templates = myContext.getTemplates();
        Map<String, Object> headers = new HashMap<>();
        setupPreviousHeaders(message,headers);
        setupKongsiMap(headers, kongsi);
        for (String template : templates) {
            // TemplateService에서 분기함..
            requestBodyAndHeaders2(rabbitTemplate, template, key,headers);
        }
    }

    private void requestCollectMetaKongsi(String key, Map<String, Object> headers) {
        requestBodyAndHeaders2(rabbitTemplate,"KONGSI_CHECK", key,headers);
        log.info(key+" >>> KONGSI_CHECK");
    }


    private Map<String, Object> headers(Map<String, Object> kongsi, String path) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(NewsConstants.KONGSI_FILE_PATH, path);
        setupKongsiMap(headers, kongsi);
        return headers;
    }


}
