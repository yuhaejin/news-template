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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 공시 파일과 텍스트 파일 처리
 */
@Slf4j
@Service
public class KongsiCollector extends DefaultService {
    @Autowired
    private MyContext myContext;
    @Autowired
    private CamelContext camelContext;

    @Autowired
    private LoadBalancerCommandHelper loadBalancerCommandHelper;

    @Autowired
    private RabbitTemplate rabbitTemplate;
    public KongsiCollector(TemplateMapper templateMapper) {
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

            // 기초공시 정보 수집
            Map<String,Object> kongsiMap = myContext.findKongsiMap(exchange);
            if (kongsiMap == null) {
                kongsiMap = findKongsiHalder2(docNo, code, acptNo);
            }

            if (kongsiMap == null) {
                return;
            }
            String docUrl = Maps.getValue(kongsiMap, "doc_url");
            // 공시 원본 파일 체크 및 처리.
            String filePath = BizUtils.htmlFilePath(myContext.getHtmlTargetPath(), docNo, code, "htm");
            File htmlFile = new File(filePath);
            String content = null;
            if (htmlFile.exists()) {
                if (BizUtils.isValid(htmlFile)) {
                    //유효한 파일이라면 그대로 읽기
                    content = FileUtils.readFileToString(htmlFile, "UTF-8");
                    log.info("공시 정상파일 존재.. "+htmlFile+" fileSize="+content.length());
                } else {
                    // 파일 수집과 저장.
                    content = findBody(loadBalancerCommandHelper, docUrl);
                    log.info("공시 비정상파일 존재.. "+htmlFile+" fileSize="+content.length());
                    saveFile(filePath, content);
                }
            } else {
                // 파일 수집과 저장.
                content = findBody(loadBalancerCommandHelper, docUrl);
                log.info("공시파일 미존재.. "+htmlFile+" fileSize="+content.length());
                saveFile(filePath, content);
            }

            // 텍스트 파일 체크 및 처리..
            String text = convertText(content);
            log.info("텍스트파일 처리... "+htmlFile+" textSize="+text.length());
            String txtPath = findPath(myContext.getHtmlTargetPath(), code, docNo, "txt");
            File txtFile = new File(txtPath);
            boolean reindexing = false;
            if (txtFile.exists()) {
                if (txtFile.length() != text.getBytes("UTF-8").length) {
                    // 텍스트파일이 크기가 다르다면..
                    log.info("파일존재하지만 크기가 달라 재처리.. "+txtPath);
                    saveFile(txtPath, text);
                    reindexing = true;
                } else {
                    reindexing = false;
                }
            } else {
                // 파일이 없다면..
                log.info("파일 미존재.. "+txtPath);
                saveFile(txtPath, text);
                reindexing = true;
            }
            log.info("공시파일 처리 완료..."+key);
            noticeKongsiCorrectComplete(key, kongsiMap,reindexing);

        } catch (Exception e) {
            handleError(rabbitTemplate, exchange, e, log);
        }
    }

    private String convertText(String content) {
        return BizUtils.extractText(content);
    }
    private String findBody(LoadBalancerCommandHelper loadBalancerCommandHelper, String docUrl) throws IOException {
        log.info("공시파일 수집.. "+docUrl);
        if (loadBalancerCommandHelper == null) {
            return JSoupHelper.findBody(docUrl);
        } else {
            return loadBalancerCommandHelper.findBody(docUrl);
        }
    }
    private void saveFile(String filePath, String content) throws IOException {
        log.info("파일저장.. "+filePath);
        FileUtils.write(new File(filePath), content, "UTF-8", false);
    }

    private String tnsDt(String tnsDt, String day) {
        if (tnsDt.length() == 4) {
            if (day.length() >= 8) {
                tnsDt = day.substring(0, 8) + tnsDt;
            } else {
                // 이런 경우는 없음..
                tnsDt = day + tnsDt;
            }
        }

        return tnsDt;
    }
    private void noticeKongsiCorrectComplete(String key, Map<String, Object> kongsiMap,boolean reindexing) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(NewsConstants.KONGSI_CHECK, NewsConstants.KONGSI_CHECK);
        if (reindexing) {
            headers.put(NewsConstants.REINDEXING, NewsConstants.REINDEXING);
        }

        setupKongsiMap(headers,kongsiMap);
        requestBodyAndHeaders2(rabbitTemplate,"OK_GATEWAY", key,headers );
//        requestBodyAndHeaders(camelContext,"rabbitmq:OK_GATEWAY?queue=OK_GATEWAY&routingKey=OK_GATEWAY&autoDelete=false", key,headers );

    }

    private Kongsi from(Map<String, Object> kongsiMap) {
        Kongsi kongsi = new Kongsi();
        kongsi.setRpt_nm(Maps.getValue(kongsiMap,"rpt_nm"));
        kongsi.setTns_dt(Maps.getValue(kongsiMap,"tns_dt"));
        kongsi.setAcpt_no(Maps.getValue(kongsiMap,"acpt_no"));

        return kongsi;
    }

    private void collectMetaKongsi(String docNo, String code, String acptNo, Map<String, Object> kongsiMap) throws Exception {
        log.debug("... collectMetaKongsi code="+code+" acptNo="+acptNo);
        String day = acptNo.substring(0, 8);
        KrxCrawler krxCrawler = new KrxCrawler();
        Kongsi kongsi = from(kongsiMap);
        krxCrawler.addUrlInfo(kongsi, loadBalancerCommandHelper, acptNo);
        String lastJson = JsonUtils.toJson(kongsi);
        String jsonPath = findMetaKongsiPath(kongsi, code, day);

        FileUtils.write(new File(jsonPath),lastJson,"UTF-8",false);
    }

    private String findMetaKongsiPath(Kongsi kongsi,String code,String day) {
        String name = kongsi.getItem_name();
        if (StringUtils.isEmpty(name)) {
            name = "SPACE";
        }
        if (StringUtils.isEmpty(code)) {
            code = "SPACE";
        }
        String _name = kongsi.getSubmit_oblg_nm();
        return myContext.getJsonTargetPath() + "/" + day + "/" + kongsi.getAcpt_no() + "_" +code+"_"+ name+"_" +_name+ ".json";
    }

}
